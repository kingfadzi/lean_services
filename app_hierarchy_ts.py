import psycopg2
import pandas as pd
from collections import defaultdict
import os

# --- DB connection config ---
DB_CONFIG = {
    "host": os.getenv("PGHOST", "helios"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": os.getenv("PGDATABASE", "lct_data"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

# --- SQL query ---
SQL_QUERY = """
    SELECT
        lca.lean_control_service_id,
        lpbd.jira_backlog_id,
        bs.service_correlation_id,
        bs.service,
        child_app.correlation_id AS app_correlation_id,
        child_app.business_application_name,
        si.correlation_id AS instance_correlation_id,
        si.it_service_instance,
        si.environment,
        si.install_type,
        child_app.application_parent_correlation_id,
        child_app.application_type,
        child_app.application_tier,
        child_app.architecture_type
    FROM public.vwsfitbusinessservice AS bs
    JOIN public.lean_control_application AS lca
      ON lca.servicenow_app_id = bs.service_correlation_id
    JOIN public.vwsfitserviceinstance AS si
      ON bs.it_business_service_sysid = si.it_business_service_sysid
    JOIN public.lean_control_product_backlog_details AS lpbd
      ON lpbd.lct_product_id = lca.lean_control_service_id
     AND lpbd.is_parent = TRUE
    JOIN public.vwsfbusinessapplication AS child_app
      ON si.business_application_sysid = child_app.business_application_sys_id
"""

# --- Connect and fetch data ---
def fetch_data():
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql_query(SQL_QUERY, conn)
    return df.drop_duplicates(subset=["app_correlation_id"])

# --- Build and print application tree ---
def build_app_tree(df):
    # app_id → name
    app_names = {
        row["app_correlation_id"]: row["business_application_name"]
        for _, row in df.iterrows()
    }

    # parent_id → [child_ids]
    children_map = defaultdict(list)
    parent_map = {}

    for _, row in df.iterrows():
        app_id = row["app_correlation_id"]
        parent_id = row["application_parent_correlation_id"]
        parent_map[app_id] = parent_id
        if parent_id and parent_id != app_id:
            children_map[parent_id].append(app_id)

    all_app_ids = set(app_names.keys())
    referenced_parents = set(parent_map.values()) - {None}

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

# --- Run ---
if __name__ == "__main__":
    df = fetch_data()
    build_app_tree(df)
