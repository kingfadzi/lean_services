import psycopg2
import pandas as pd
from collections import defaultdict
import os

# --- Database connection config (your values) ---
DB_CONFIG = {
    "host": os.getenv("PGHOST", "helios"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": os.getenv("PGDATABASE", "lct_data"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

# --- SQL Query ---
SQL_QUERY = """
    SELECT
        bac.correlation_id AS app_correlation_id,
        bac.business_application_name,
        bac.application_parent_correlation_id
    FROM public.vwsfitserviceinstance AS si
    JOIN public.lean_control_application AS fia
      ON fia.servicenow_app_id = si.correlation_id
    JOIN (
        SELECT DISTINCT ON (lct_product_id)
            lct_product_id,
            jira_backlog_id
        FROM public.lean_control_product_backlog_details
        WHERE is_parent = TRUE
        ORDER BY lct_product_id, jira_backlog_id
    ) AS lpbd_dedup
      ON lpbd_dedup.lct_product_id = fia.lean_control_service_id
    JOIN public.vwsfbusinessapplication AS bac
      ON si.business_application_sysid = bac.business_application_sys_id
    JOIN public.vwsfitbusinessservice AS bs
      ON si.it_business_service_sysid = bs.it_business_service_sysid
"""

# --- Fetch data from Postgres ---
def fetch_data():
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql_query(SQL_QUERY, conn)
    return df.drop_duplicates(subset=["app_correlation_id"])

# --- Build and print application tree ---
def build_app_tree(df):
    # Map: app_id → name
    app_names = {
        row["app_correlation_id"]: row["business_application_name"]
        for _, row in df.iterrows()
    }

    # Map: parent_id → [child_ids]
    children_map = defaultdict(list)
    parent_map = {}

    for _, row in df.iterrows():
        app_id = row["app_correlation_id"]
        parent_id = row["application_parent_correlation_id"]
        parent_map[app_id] = parent_id
        if parent_id and parent_id != app_id:
            children_map[parent_id].append(app_id)

    all_app_ids = set(app_names.keys())
    all_parent_ids = set(parent_map.values()) - {None}

    # Root = app whose parent is null OR parent not in dataset
    root_apps = [
        app_id for app_id in all_app_ids
        if parent_map.get(app_id) is None or parent_map[app_id] not in all_app_ids
    ]

    visited = set()

    def print_tree(app_id, level=0):
        if app_id in visited:
            print("  " * level + f"- [CYCLE] {app_names.get(app_id, app_id)} ({app_id})")
            return
        visited.add(app_id)
        print("  " * level + f"- {app_names.get(app_id, app_id)} ({app_id})")
        for child_id in sorted(children_map.get(app_id, [])):
            print_tree(child_id, level + 1)

    for root in sorted(root_apps):
        print_tree(root)

# --- Run it ---
if __name__ == "__main__":
    df = fetch_data()
    build_app_tree(df)
