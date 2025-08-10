import asyncio
import yaml
import aioodbc
import asyncpg
import re
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Union, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from filelock import FileLock
import importlib
import hashlib

CHUNK_SIZE = 500
CONFIG_PATH = "metadata/table_mapping.yaml"
OFFSET_FILE = Path("migration_checkpoints.json")

# === Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")

# === Configuration Handling ===
def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

# === Checkpoint Management (KEYED) ===
def _ckpt_key(src_conn: str, dst_conn: str, src_table: str, dst_table: str) -> str:
    h = hashlib.sha1((src_conn + "→" + dst_conn).encode()).hexdigest()[:8]
    return f"{src_table}->{dst_table}#{h}"

def _read_ckpt_file() -> Dict[str, Any]:
    with FileLock(str(OFFSET_FILE) + ".lock"):
        if not OFFSET_FILE.exists():
            return {}
        try:
            return json.loads(OFFSET_FILE.read_text())
        except json.JSONDecodeError:
            return {}

def _write_ckpt_file(data: Dict[str, Any]) -> None:
    with FileLock(str(OFFSET_FILE) + ".lock"):
        temp = OFFSET_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(data))
        temp.replace(OFFSET_FILE)

def read_checkpoint_keyed(src_conn: str, dst_conn: str, src_table: str, dst_table: str) -> int:
    key = _ckpt_key(src_conn, dst_conn, src_table, dst_table)
    data = _read_ckpt_file()
    return int(data.get(key, 0))

def write_checkpoint_keyed(src_conn: str, dst_conn: str, src_table: str, dst_table: str, offset: int) -> None:
    key = _ckpt_key(src_conn, dst_conn, src_table, dst_table)
    data = _read_ckpt_file()
    data[key] = int(offset)
    _write_ckpt_file(data)

# === Schema Handling ===
def normalize_col(name: str) -> str:
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name.strip()).lower()
    return re.sub(r'_+', '_', name).strip('_')[:63]

def sqlserver_to_postgres_type(sql_type: str, max_len: Union[int, None]) -> str:
    t = sql_type.lower()
    if t in ("varchar", "nvarchar", "char", "nchar"):
        return f"varchar({max_len})" if max_len and max_len > 0 else "text"
    return {
        "int": "integer", "bigint": "bigint", "smallint": "smallint", "bit": "boolean",
        "decimal": "numeric", "numeric": "numeric", "float": "double precision",
        "real": "real", "uniqueidentifier": "uuid", "varbinary": "bytea",
        "datetime": "timestamptz", "datetime2": "timestamptz",
        "date": "date", "time": "time", "smalldatetime": "timestamptz"
    }.get(t, "text")

async def extract_sqlserver_schema(conn_str: str, table: str, selected_columns: Optional[List[str]] = None) -> str:
    """
    Build a CREATE TABLE DDL for Postgres based on SQL Server schema.
    If selected_columns is provided, only include those columns (case-insensitive),
    ordered as provided in selected_columns.
    """
    schema, name = table.split(".")
    conn = await aioodbc.connect(dsn=conn_str, autocommit=True)
    cursor = await conn.cursor()
    try:
        await cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (schema, name))
        rows = await cursor.fetchall()

        # Filter/Order columns if a subset is specified
        if selected_columns and len(selected_columns) > 0:
            want = [c.strip() for c in selected_columns]
            want_lc_index = {c.lower(): i for i, c in enumerate(want)}
            # Keep only wanted columns (case-insensitive match)
            rows = [
                r for r in rows
                if r[0].lower() in want_lc_index
            ]
            # Order rows according to provided selected_columns
            rows.sort(key=lambda r: want_lc_index[r[0].lower()])

        ddl_lines = []
        for col_name, col_type, nullable, char_max, _pos in rows:
            norm_col = normalize_col(col_name)
            pg_type = sqlserver_to_postgres_type(col_type, char_max)
            line = f'"{norm_col}" {pg_type}' + (" NOT NULL" if nullable == "NO" else "")
            ddl_lines.append(line)

        target_schema = normalize_col(schema)
        target_name = normalize_col(name)
        return f'CREATE TABLE "{target_schema}"."{target_name}" (\n  ' + ",\n  ".join(ddl_lines) + "\n);"
    finally:
        await cursor.close()
        await conn.close()

async def recreate_pg_table(conn_str: str, ddl: str, target_table: str):
    target_schema, target_name = target_table.split(".", 1)
    norm_schema = normalize_col(target_schema)
    norm_name = normalize_col(target_name)
    fqn = f'"{norm_schema}"."{norm_name}"'
    # ddl already contains column list; replace only the table FQN safely
    m = re.search(r"\(.*\)", ddl, flags=re.DOTALL)
    if not m:
        raise ValueError("Could not find column list in DDL:\n" + ddl[:200])
    fixed_ddl = f"CREATE TABLE {fqn} {m.group(0)}"
    logger.info("Recreate DDL:\n" + repr(fixed_ddl))
    conn = await asyncpg.connect(conn_str)
    try:
        async with conn.transaction():
            await conn.execute(f"DROP TABLE IF EXISTS {fqn} CASCADE")
            await conn.execute(fixed_ddl)
    finally:
        await conn.close()

# === Data Fetch/Insert ===
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=15, min=15, max=120),
    retry=retry_if_exception_type(Exception)
)
async def fetch_chunk_sqlserver(conn_str: str, table: str, sort_columns: List[str], offset: int, limit: int,
                                where: str = None, selected_columns: Optional[List[str]] = None) -> List[dict]:
    """
    Fetch a chunk from SQL Server with OFFSET/FETCH.
    If selected_columns is provided, only select those columns; otherwise SELECT *.
    sort_columns are required for deterministic pagination.
    """
    if not sort_columns:
        raise ValueError("Sort columns required")

    schema, name = table.split(".", 1)

    def escape_sql_id(identifier: str) -> str:
        return f"[{identifier.replace(']', ']]')}]"

    esc_schema = escape_sql_id(schema)
    esc_table = escape_sql_id(name)
    esc_order = ", ".join([escape_sql_id(col) for col in sort_columns])

    if selected_columns and len(selected_columns) > 0:
        # Build a SELECT list with escaped identifiers
        esc_select = ", ".join([escape_sql_id(c) for c in selected_columns])
        select_clause = esc_select
    else:
        select_clause = "*"

    sql = f"""
        SELECT {select_clause}
        FROM {esc_schema}.{esc_table}
        {f"WHERE {where}" if where else ""}
        ORDER BY {esc_order}
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """

    async with aioodbc.connect(dsn=conn_str, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (offset, limit))
            if not cur.description:
                return []
            cols = [col[0] for col in cur.description]
            return [dict(zip(cols, row)) async for row in cur]

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=10, min=10, max=60),
    retry=retry_if_exception_type(Exception)
)
async def insert_chunk_pg(conn_str: str, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    schema, name = table.split(".", 1)
    raw_cols = list(rows[0].keys())
    norm_cols = [normalize_col(c) for c in raw_cols]
    conn = await asyncpg.connect(conn_str)
    try:
        await conn.copy_records_to_table(
            table_name=normalize_col(name),
            schema_name=normalize_col(schema),
            records=[tuple(row[col] for col in raw_cols) for row in rows],
            columns=norm_cols
        )
        return len(rows)
    finally:
        await conn.close()

def apply_plugin_chain(rows: List[dict], plugin_paths: List[str]) -> List[dict]:
    for plugin_path in plugin_paths:
        mod_name, func_name = plugin_path.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        rows = func(rows)
    return rows

# === Orchestration ===
async def migrate_table(src_conn: str, dst_conn: str, src_table: str, dst_table: str,
                        sort_columns: List[str], table_cfg: dict, where: str = None):
    logger.info(f"Starting migration: {src_table} → {dst_table}")

    # Columns to select (optional)
    selected_cols = table_cfg.get("columns")  # e.g., ["Id","Name","UpdatedAt"]

    # Read keyed checkpoint
    offset = read_checkpoint_keyed(src_conn, dst_conn, src_table, dst_table)
    recreate = table_cfg.get("recreate", True)

    # Only (re)create the target table if starting fresh OR explicitly requested
    if offset == 0 and recreate:
        try:
            ddl = await extract_sqlserver_schema(src_conn, src_table, selected_columns=selected_cols)
            await recreate_pg_table(dst_conn, ddl, dst_table)
            post_file = table_cfg.get("post_ddl_file")
            if post_file:
                post_sql = Path(post_file).read_text()
                conn = await asyncpg.connect(dst_conn)
                try:
                    await conn.execute(post_sql)
                finally:
                    await conn.close()
        except Exception as e:
            logger.error(f"DDL error: {e}")
            raise
    else:
        logger.info(f"Resuming {src_table} at offset {offset} without dropping {dst_table}")

    total = 0
    while True:
        try:
            rows = await fetch_chunk_sqlserver(
                conn_str=src_conn,
                table=src_table,
                sort_columns=sort_columns,
                offset=offset,
                limit=CHUNK_SIZE,
                where=where,
                selected_columns=selected_cols
            )
            if not rows:
                break

            plugins = table_cfg.get("transforms", {}).get("plugins", [])
            if plugins:
                rows = apply_plugin_chain(rows, plugins)

            inserted = await insert_chunk_pg(dst_conn, dst_table, rows)
            total += inserted

            # Advance offset/checkpoint only after successful insert
            offset += inserted
            write_checkpoint_keyed(src_conn, dst_conn, src_table, dst_table, offset)

            logger.info(f"Copied {inserted} rows (offset now {offset}, total {total})")

        except Exception as e:
            # No rewind: checkpoint already reflects last successful insert
            logger.error(f"Error at offset {offset}: {e}")
            raise

    # Completed this table successfully; clear checkpoint
    write_checkpoint_keyed(src_conn, dst_conn, src_table, dst_table, 0)
    logger.info(f"Finished migration: {total} rows")

async def main_migration():
    config = load_config()
    for db_group in config.get("databases", []):
        if not db_group.get("enabled", True):
            continue
        tasks = []
        for table_cfg in db_group.get("tables", []):
            if not table_cfg.get("enabled", True):
                continue
            tasks.append(
                migrate_table(
                    db_group["source_db"],
                    db_group["target_db"],
                    table_cfg["source"],
                    table_cfg["dest"],
                    table_cfg["sort_columns"],
                    table_cfg,
                    where=table_cfg.get("where")
                )
            )
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main_migration())
