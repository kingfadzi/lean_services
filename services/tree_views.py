from django.shortcuts import render
from django.db import connection
from django.core.paginator import Paginator
from collections import defaultdict
import pandas as pd


def fetch_records(mode):
    if mode == "by_ts":
        query = """
            SELECT
                lca.lean_control_service_id,
                lpbd.jira_backlog_id,
                bs.service_correlation_id,
                bs.service,
                child_app.correlation_id AS app_correlation_id,
                child_app.business_application_name AS app_name,
                si.correlation_id AS instance_correlation_id,
                si.it_service_instance AS instance_name,
                si.environment,
                si.install_type,
                child_app.application_parent_correlation_id AS parent_app_id,
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
    else:
        query = """
            SELECT
                fia.lean_control_service_id,
                lpbd_dedup.jira_backlog_id,
                bs.service_correlation_id,
                bs.service,
                bac.correlation_id AS app_correlation_id,
                bac.business_application_name AS app_name,
                si.correlation_id AS instance_correlation_id,
                si.it_service_instance AS instance_name,
                si.environment,
                si.install_type,
                bac.application_parent_correlation_id AS parent_app_id,
                bac.application_type,
                bac.application_tier,
                bac.architecture_type
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

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    columns = [
        "lean_control_service_id", "jira_backlog_id", "service_id", "service_name",
        "app_correlation_id", "app_name", "instance_correlation_id", "instance_name",
        "environment", "install_type", "parent_app_id",
        "application_type", "application_tier", "architecture_type"
    ]
    return pd.DataFrame(rows, columns=columns)


def build_tree(df):
    from collections import defaultdict

    nodes = {}
    parent_map = {}
    children_map = defaultdict(set)  # Use set to avoid duplicates

    for _, row in df.iterrows():
        app_id = row["app_correlation_id"]
        parent_id = row["parent_app_id"]
        parent_map[app_id] = parent_id

        # Create node only once
        if app_id not in nodes:
            nodes[app_id] = {
                "id": app_id,
                "name": row["app_name"],
                "type": row["application_type"],
                "tier": row["application_tier"],
                "architecture": row["architecture_type"],
                "lean_control_id": row["lean_control_service_id"],
                "jira_backlog_id": row["jira_backlog_id"],
                "service_name": row["service_name"],
                "service_id": row["service_id"],
                "children": [],
                "instances": {},
            }

        # Add instance if not already added
        instance_id = row["instance_correlation_id"]
        if instance_id and instance_id not in nodes[app_id]["instances"]:
            nodes[app_id]["instances"][instance_id] = {
                "id": instance_id,
                "name": row["instance_name"],
                "env": row["environment"],
                "install_type": row["install_type"],
            }

        # Track parent-child relationships
        if parent_id and parent_id != app_id:
            children_map[parent_id].add(app_id)

    # Link children to parents (avoid duplicates)
    for parent_id, child_ids in children_map.items():
        if parent_id not in nodes:
            print(f"[DEBUG] Skipping unknown parent_id: {parent_id}")
            continue
        for child_id in child_ids:
            if child_id not in nodes:
                print(f"[DEBUG] Skipping unknown child_id: {child_id}")
                continue
            if nodes[child_id] not in nodes[parent_id]["children"]:
                nodes[parent_id]["children"].append(nodes[child_id])

    # Convert instance dicts to lists
    for node in nodes.values():
        node["instances"] = list(node["instances"].values())

    # Identify root nodes
    all_app_ids = set(nodes.keys())
    root_ids = [
        app_id for app_id in all_app_ids
        if parent_map.get(app_id) is None or parent_map[app_id] not in all_app_ids
    ]

    print(f"[DEBUG] Root App Count: {len(root_ids)}")

    return [nodes[root_id] for root_id in root_ids], nodes




def application_tree_view(request):
    mode = request.GET.get("mode", "by_si")
    page_number = request.GET.get("page", 1)

    df = fetch_records(mode)
    root_nodes, node_map = build_tree(df)

    paginator = Paginator([node["id"] for node in root_nodes], 10)
    page_obj = paginator.get_page(page_number)
    page_roots = page_obj.object_list

    paginated_trees = [node_map[root_id] for root_id in page_roots if root_id in node_map]

    return render(request, "application_tree/tree_view.html", {
        "trees": paginated_trees,
        "mode": mode,
        "page_obj": page_obj,
    })
