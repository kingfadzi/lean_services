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
                child_app.business_application_name,
                si.correlation_id AS instance_correlation_id,
                si.it_service_instance,
                si.environment,
                si.install_type,
                child_app.application_parent_correlation_id AS application_parent_correlation_id,
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
                bac.business_application_name,
                si.correlation_id AS instance_correlation_id,
                si.it_service_instance,
                si.environment,
                si.install_type,
                bac.application_parent_correlation_id AS application_parent_correlation_id,
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
        "environment", "install_type", "application_parent_correlation_id",
        "application_type", "application_tier", "architecture_type"
    ]

    df = pd.DataFrame(rows, columns=columns)
    df.sort_values("app_name", inplace=True)
    return df


def fetch_repositories(app_ids):
    """Get repos grouped by tool for the given app IDs, safely."""
    # Drop null/empty and cast to plain Python strings
    clean_ids = [str(a) for a in app_ids if a not in (None, "", "None")]
    if not clean_ids:
        return {}

    with connection.cursor() as cursor:
        query = """
            SELECT
                cm1.identifier AS app_id,
                cm2.tool_type,
                cm2.identifier AS repo_name
            FROM public.component_mapping cm1
            JOIN public.component_mapping cm2
              ON cm1.component_id = cm2.component_id
            WHERE cm1.mapping_type = 'it_business_application'
              AND cm2.mapping_type = 'version_control'
              AND cm1.identifier = ANY(%s)
        """
        cursor.execute(query, [clean_ids])
        rows = cursor.fetchall()

    repos_by_app = defaultdict(lambda: defaultdict(list))
    for app_id, tool_type, repo_name in rows:
        if repo_name:  # ignore empty repo names
            repos_by_app[app_id][tool_type].append(repo_name)

    return repos_by_app


def build_tree(df, search_term=None):
    df = df.copy()

    if search_term:
        s = search_term.lower()
        mask = df.apply(lambda row: s in str(row.values).lower(), axis=1)
        matching_apps = set(df[mask]["app_correlation_id"])
        matching_parents = set(df[df["app_correlation_id"].isin(matching_apps)]["application_parent_correlation_id"])
        keep_apps = matching_apps.union(matching_parents)
        df = df[df["app_correlation_id"].isin(keep_apps)]

    app_ids = df["app_correlation_id"].unique().tolist()
    repos_by_app = fetch_repositories(app_ids)

    # Group service instances by app and environment
    instances_by_app = defaultdict(lambda: defaultdict(list))
    for _, row in df.iterrows():
        app_id = row["app_correlation_id"]
        env = row["environment"]
        inst = {
            "id": row["instance_correlation_id"],
            "name": row["instance_name"],
            "install_type": row["install_type"],
        }
        instances_by_app[app_id][env].append(inst)

    nodes = {}
    parent_map = {}
    children_map = defaultdict(list)

    for _, row in df.drop_duplicates("app_correlation_id").iterrows():
        app_id = row["app_correlation_id"]
        parent_id = row["application_parent_correlation_id"]

        repo_groups = repos_by_app.get(app_id, {})
        # ⚙️ normalize, drop empties, sort & dedupe just in case
        repo_groups = {
            str(tool): sorted({r for r in (repos or []) if r})
            for tool, repos in repo_groups.items()
            if repos
        }
        repo_count = sum(len(v) for v in repo_groups.values())

        # ⚙️ create a stable list of (tool, repos) for the template to iterate
        repo_pairs = sorted(repo_groups.items(), key=lambda t: t[0].lower())

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
            "instances_grouped": dict(instances_by_app.get(app_id, {})),
            "repositories": repo_groups,   # keep dict if you need it elsewhere
            "repo_pairs": repo_pairs,      # ⚙️ use this in template
            "repo_count": repo_count,
            "children": [],
        }

        parent_map[app_id] = parent_id
        if parent_id and parent_id != app_id:
            children_map[parent_id].append(app_id)

    for parent_id, child_ids in children_map.items():
        if parent_id in nodes:
            for child_id in child_ids:
                if child_id in nodes:
                    nodes[parent_id]["children"].append(nodes[child_id])

    root_ids = [
        app_id for app_id in nodes
        if parent_map.get(app_id) is None or parent_map[app_id] not in nodes
    ]

    root_nodes = [nodes[app_id] for app_id in sorted(root_ids, key=lambda aid: nodes[aid]["name"])]
    return root_nodes


def application_tree_view(request):
    mode = request.GET.get("mode", "by_si")
    search_term = request.GET.get("search", "").strip()
    page_number = request.GET.get("page", 1)

    df = fetch_records(mode)
    tree = build_tree(df, search_term)

    paginator = Paginator(tree, 25)
    page_obj = paginator.get_page(page_number)

    return render(request, "application_tree/tree_view.html", {
        "trees": page_obj.object_list,
        "mode": mode,
        "search": search_term,
        "page_obj": page_obj,
    })
