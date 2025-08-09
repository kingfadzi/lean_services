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
CONFIG_PATH = "table_mapping.yaml"
OFFSET_FILE = Path("migration_checkpoints.json")

# === Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")

# === Configuration Handling ===
def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

# === Checkpoint Management (OFFSET for tables) ===
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

# === Keyset Checkpoints (generic; per dest) ===
def read_keyset_checkpoint(dest: str) -> Dict[str, Any] | None:
    try:
        with FileLock(str(OFFSET_FILE) + ".lock"):
            if not OFFSET_FILE.exists():
                return None
            data = json.loads(OFFSET_FILE.read_text())
            return data.get(f"keyset::{dest}")
    except (json.JSONDecodeError, FileNotFoundError):
        return None

def write_keyset_checkpoint(dest: str, sort_value: Any, unique_value: Any):
    with FileLock(str(OFFSET_FILE) + ".lock"):
        data = {}
        if OFFSET_FILE.exists():
            data = json.loads(OFFSET_FILE.read_text())
        data[f"keyset::{dest}"] = {"sort": sort_value, "unique": unique_value}
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

# === Helpers for query-vs-table and WHERE wrapping ===
def _is_sql_query(source: str) -> bool:
    s = source.strip().lower()
    return s.startswith("select") or s.startswith("with")

def _wrap_query_with_where(base_sql: str, where: str | None) -> str:
    if not where:
        return f"({base_sql}) AS q"
    return f"(SELECT * FROM ({base_sql}) AS q_base WHERE {where}) AS q"

# === Fetchers ===
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

async def _fetch_query_offset(conn_str: str, from_sql: str, order_by: str, offset: int, limit: int) -> List[dict]:
    sql = f"""
        SELECT * FROM {from_sql}
        ORDER BY {order_by}
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """
    async with aioodbc.connect(dsn=conn_str, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (offset, limit))
            if not cur.description:
                return []
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) async for r in cur]

async def fetch_keyset_chunk_generic(
        conn_str: str,
        from_sql: str,             # "(<query>) AS q" OR table FQN used as subquery
        sort_field: str,
        unique_field: str,
        direction: str,            # "asc" | "desc"
        last_sort_value,
        last_unique_value,
        limit: int
) -> List[dict]:
    dir_lower = (direction or "asc").lower()
    if dir_lower not in ("asc", "desc"):
        raise ValueError("order must be 'asc' or 'desc'")

    order_clause = f"ORDER BY {sort_field} {dir_lower}, {unique_field} {dir_lower}"

    # On first chunk (no cursor), omit WHERE and just order+fetch
    if last_sort_value is None or last_unique_value is None:
        query = f"SELECT * FROM {from_sql} {order_clause} OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
        params = (limit,)
    else:
        if dir_lower == "asc":
            predicate = f"(({sort_field} > ?) OR ({sort_field} = ? AND {unique_field} > ?))"
        else:
            predicate = f"(({sort_field} < ?) OR ({sort_field} = ? AND {unique_field} < ?))"

        query = f"""
            SELECT * FROM (
                SELECT * FROM {from_sql}
                WHERE {predicate}
            ) AS chunked
            {order_clause}
            OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """
        params = (last_sort_value, last_sort_value, last_unique_value, limit)

    async with aioodbc.connect(dsn=conn_str, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            if not cur.description:
                return []
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) async for row in cur]

# === Inserter ===
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

# === Transforms ===
def apply_plugin_chain(rows: list[dict], plugin_paths: list[str]) -> list[dict]:
    for plugin_path in plugin_paths:
        mod_name, func_name = plugin_path.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        rows = func(rows)
    return rows

# === Table migration (preserved semantics) ===
async def migrate_table(src_conn: str, dst_conn: str, src_table: str, dst_table: str, sort_columns: List[str], table_cfg: dict, where: str = None):
    logger.info(f"Starting migration: {src_table} → {dst_table}")
    try:
        # Optional pre-DDL (new; runs BEFORE recreate)
        pre_file = table_cfg.get("pre_ddl_file")
        if pre_file:
            conn = await asyncpg.connect(dst_conn)
            try:
                await conn.execute(Path(pre_file).read_text())
            finally:
                await conn.close()

        # Preserve original behavior: extract, recreate, then run post_ddl_file BEFORE copy
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

# === Unified entry migration (table OR query) ===
async def migrate_entry(src_conn: str, dst_conn: str, cfg: dict):
    """
    Uses the SAME config item shape as tables[].
    If cfg['source'] starts with SELECT/WITH => treat as query.
    Else => treat as table (preserve existing behavior).
    """
    source = cfg["source"]
    dest = cfg["dest"]
    where = cfg.get("where")
    sort_columns = cfg.get("sort_columns") or []
    order = cfg.get("order", "asc").lower()
    plugins = cfg.get("transforms", {}).get("plugins", [])

    if not _is_sql_query(source):
        # TABLE path: untouched semantics (plus optional pre_ddl_file support)
        await migrate_table(
            src_conn, dst_conn, source, dest, sort_columns, cfg, where=where
        )
        return

    # QUERY path
    logger.info(f"Starting query migration: source SQL → {dest}")

    # Optional schema setup for queries:
    pre_file = cfg.get("pre_ddl_file")
    if pre_file:
        conn = await asyncpg.connect(dst_conn)
        try:
            await conn.execute(Path(pre_file).read_text())
        finally:
            await conn.close()

    # Preserve post_ddl_file timing as "pre-load" (mirrors table behavior)
    post_file = cfg.get("post_ddl_file")
    if post_file:
        conn = await asyncpg.connect(dst_conn)
        try:
            await conn.execute(Path(post_file).read_text())
        finally:
            await conn.close()

    # Build FROM clause with optional WHERE
    from_sql = _wrap_query_with_where(source, where)

    total = 0
    # Keyset if at least 2 columns; else OFFSET if 1 column
    if len(sort_columns) >= 2:
        sort_field, unique_field = sort_columns[0], sort_columns[1]
        cursor = read_keyset_checkpoint(dest)
        last_sort = cursor["sort"] if cursor else None
        last_unique = cursor["unique"] if cursor else None

        while True:
            rows = await fetch_keyset_chunk_generic(
                conn_str=src_conn,
                from_sql=from_sql,
                sort_field=sort_field,
                unique_field=unique_field,
                direction=order,
                last_sort_value=last_sort,
                last_unique_value=last_unique,
                limit=CHUNK_SIZE
            )
            if not rows:
                break
            if plugins:
                rows = apply_plugin_chain(rows, plugins)
            inserted = await insert_chunk_pg(dst_conn, dest, rows)
            total += inserted
            # advance cursor
            last_row = rows[-1]
            last_sort = last_row[sort_field]
            last_unique = last_row[unique_field]
            write_keyset_checkpoint(dest, last_sort, last_unique)
            logger.info(f"{dest}: inserted {inserted} (total {total}) "
                        f"cursor=({sort_field}={last_sort}, {unique_field}={last_unique})")

    else:
        if not sort_columns:
            raise ValueError("For queries, provide sort_columns (1 for OFFSET, 2 for keyset).")
        order_by = ", ".join(sort_columns) + (f" {order}" if order in ("asc", "desc") else "")
        offset = 0
        while True:
            rows = await _fetch_query_offset(src_conn, from_sql, order_by, offset, CHUNK_SIZE)
            if not rows:
                break
            if plugins:
                rows = apply_plugin_chain(rows, plugins)
            inserted = await insert_chunk_pg(dst_conn, dest, rows)
            total += inserted
            offset += len(rows)
            logger.info(f"{dest}: inserted {inserted} (total {total}) offset={offset}")

    logger.info(f"Finished query migration → {dest}: {total} rows")

# === Orchestrator ===
async def main_migration():
    config = load_config()
    for db_group in config.get("databases", []):
        if not db_group.get("enabled", True):
            continue
        tasks = []
        for table_cfg in db_group.get("tables", []):
            if not table_cfg.get("enabled", True):
                continue
            # Unified call (table or query decided inside)
            tasks.append(
                migrate_entry(
                    db_group["source_db"],
                    db_group["target_db"],
                    table_cfg
                )
            )
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main_migration())
