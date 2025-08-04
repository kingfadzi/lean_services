import asyncio
import yaml
import aioodbc
import asyncpg
import re
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Union
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from filelock import FileLock
import importlib

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

# === Checkpoint Management ===
def read_checkpoint(table: str) -> int:
    try:
        with FileLock(str(OFFSET_FILE) + ".lock"):
            if not OFFSET_FILE.exists():
                return 0
            data = json.loads(OFFSET_FILE.read_text())
            return data.get(table, 0)
    except (json.JSONDecodeError, FileNotFoundError):
        return 0

def write_checkpoint(table: str, offset: int):
    with FileLock(str(OFFSET_FILE) + ".lock"):
        data = {}
        if OFFSET_FILE.exists():
            data = json.loads(OFFSET_FILE.read_text())
        data[table] = offset
        temp = OFFSET_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(data))
        temp.replace(OFFSET_FILE)

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

async def extract_sqlserver_schema(conn_str: str, table: str) -> str:
    schema, name = table.split(".")
    ddl_lines = []
    conn = await aioodbc.connect(dsn=conn_str, autocommit=True)
    cursor = await conn.cursor()
    try:
        await cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (schema, name))
        rows = await cursor.fetchall()
        for col_name, col_type, nullable, char_max in rows:
            norm_col = normalize_col(col_name)
            pg_type = sqlserver_to_postgres_type(col_type, char_max)
            line = f'"{norm_col}" {pg_type}' + (" NOT NULL" if nullable == "NO" else "")
            ddl_lines.append(line)
        return f'CREATE TABLE {schema}.{name} (\n  ' + ",\n  ".join(ddl_lines) + "\n);"
    finally:
        await cursor.close()
        await conn.close()

async def recreate_pg_table(conn_str: str, ddl: str, target_table: str):
    target_schema, target_name = target_table.split(".", 1)
    norm_schema = normalize_col(target_schema)
    norm_name = normalize_col(target_name)
    fqn = f'"{norm_schema}"."{norm_name}"'
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

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=15, min=15, max=120),
    retry=retry_if_exception_type(Exception)
)
async def fetch_chunk_sqlserver(conn_str: str, table: str, sort_columns: List[str], offset: int, limit: int, where: str = None) -> List[dict]:
    if not sort_columns:
        raise ValueError("Sort columns required")
    schema, name = table.split(".", 1)
    def escape_sql_id(identifier: str) -> str:
        return f"[{identifier.replace(']', ']]')}]"
    esc_schema = escape_sql_id(schema)
    esc_table = escape_sql_id(name)
    esc_cols = ", ".join([escape_sql_id(col) for col in sort_columns])
    sql = f"""
        SELECT * FROM {esc_schema}.{esc_table}
        {f"WHERE {where}" if where else ""}
        ORDER BY {esc_cols}
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
    fqn = f'"{normalize_col(schema)}"."{normalize_col(name)}"'
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

def apply_plugin_chain(rows: list[dict], plugin_paths: list[str]) -> list[dict]:
    for plugin_path in plugin_paths:
        mod_name, func_name = plugin_path.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        rows = func(rows)
    return rows

async def migrate_table(src_conn: str, dst_conn: str, src_table: str, dst_table: str, sort_columns: List[str], table_cfg: dict, where: str = None):
    logger.info(f"Starting migration: {src_table} → {dst_table}")
    try:
        ddl = await extract_sqlserver_schema(src_conn, src_table)
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
    offset = read_checkpoint(src_table)
    total = 0
    while True:
        try:
            rows = await fetch_chunk_sqlserver(src_conn, src_table, sort_columns, offset, CHUNK_SIZE, where)
            if not rows:
                break
            plugins = table_cfg.get("transforms", {}).get("plugins", [])
            if plugins:
                rows = apply_plugin_chain(rows, plugins)
            inserted = await insert_chunk_pg(dst_conn, dst_table, rows)
            total += inserted
            offset += len(rows)
            write_checkpoint(src_table, offset)
            logger.info(f"Copied {inserted} rows (total {total})")
        except Exception as e:
            logger.error(f"Error at offset {offset}: {e}")
            write_checkpoint(src_table, max(0, offset - CHUNK_SIZE))
            raise
    write_checkpoint(src_table, 0)
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
