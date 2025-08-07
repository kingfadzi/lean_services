from django.shortcuts import render
from django.db import connection
from collections import defaultdict
from django.core.paginator import Paginator
import pandas as pd

QUERIES = {
    "by_si": """
        SELECT
            fia.lean_control_service_id,
            lpbd_dedup.jira_backlog_id,
            bs.service_correlation_id,
            bs.service,
            bac.correlation_id AS app_correlation_id,
            bac.business_application_name,
            si.correlation_id AS instance_correlation_id,
            si.it_service_instance,
            si.environment,
            si.install_type,
            bac.application_parent_correlation_id,
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
    """,
    "by_ts": """
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
}

def fetch_tree_data(mode):
    query = QUERIES.get(mode)
    if not query:
        raise ValueError("Invalid mode")

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

    return pd.DataFrame(rows, columns=columns)

def build_tree(df):
    nodes = {}
    children_map = defaultdict(list)
    parent_map = {}

    for _, row in df.iterrows():
        app_id = row["app_correlation_id"]
        parent_id = row["application_parent_correlation_id"]
        name = row["business_application_name"]

        nodes.setdefault(app_id, {
            "id": app_id,
            "name": name,
            "type": row.get("application_type"),
            "tier": row.get("application_tier"),
            "architecture": row.get("architecture_type"),
            "lean_control_id": row.get("lean_control_service_id"),
            "jira_backlog_id": row.get("jira_backlog_id"),
            "service_id": row.get("service_correlation_id"),
            "service_name": row.get("service"),
            "children": [],
            "instances": []
        })

        instance = {
            "id": row.get("instance_correlation_id"),
            "name": row.get("it_service_instance"),
            "env": row.get("environment"),
            "install_type": row.get("install_type")
        }
        nodes[app_id]["instances"].append(instance)

        parent_map[app_id] = parent_id
        if parent_id and parent_id != app_id:
            children_map[parent_id].append(app_id)

    for parent_id, child_ids in children_map.items():
        for child_id in child_ids:
            if child_id in nodes:
                nodes[parent_id]["children"].append(nodes[child_id])

    all_app_ids = set(nodes.keys())
    root_ids = [app_id for app_id in all_app_ids if parent_map.get(app_id) is None or parent_map[app_id] not in all_app_ids]
    root_nodes = [nodes[root_id] for root_id in root_ids]

    return root_nodes, root_ids  # Return both full trees and root ID list

def application_tree_view(request):
    mode = request.GET.get("mode", "by_si")
    page_number = request.GET.get("page", 1)

    df = fetch_tree_data(mode)
    all_trees, root_ids = build_tree(df)

    # map app_id to node for fast lookup
    tree_map = {node["id"]: node for node in all_trees}
    paginator = Paginator(root_ids, 10)
    page_obj = paginator.get_page(page_number)
    page_roots = page_obj.object_list
    paginated_trees = [tree_map[root_id] for root_id in page_roots if root_id in tree_map]

    return render(request, "application_tree/tree_view.html", {
        "trees": paginated_trees,
        "mode": mode,
        "page_obj": page_obj,
    })
