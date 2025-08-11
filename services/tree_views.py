from django.shortcuts import render
from django.db import connection
from django.core.paginator import Paginator
from collections import defaultdict, OrderedDict
import pandas as pd


def fetch_records(mode):
    """
    Base dataset (apps, services, instances) with instance_sysid included
    so we can attach change requests later.
    """
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
                child_app.architecture_type,
                si.it_service_instance_sysid AS instance_sysid
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
                bac.architecture_type,
                si.it_service_instance_sysid AS instance_sysid
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
        "application_type", "application_tier", "architecture_type", "instance_sysid"
    ]

    df = pd.DataFrame(rows, columns=columns)
    df.sort_values("app_name", inplace=True)
    return df


def fetch_repositories(app_ids):
    """
    Get repositories grouped by tool for the given app IDs.
    Use cm2.name when present, else fall back to cm2.identifier.
    Then derive a human-friendly short name (strip path and .git).
    """
    clean_ids = [str(a) for a in app_ids if a not in (None, "", "None")]
    if not clean_ids:
        return {}

    with connection.cursor() as cursor:
        query = """
            SELECT
                cm1.identifier AS app_id,
                cm2.tool_type,
                COALESCE(cm2.name, cm2.identifier) AS repo_raw
            FROM public.component_mapping cm1
            JOIN public.component_mapping cm2
              ON cm1.component_id = cm2.component_id
            WHERE cm1.mapping_type = 'it_business_application'
              AND cm2.mapping_type = 'version_control'
              AND cm1.identifier = ANY(%s)
        """
        cursor.execute(query, [clean_ids])
        rows = cursor.fetchall()

    def friendly(n):
        if not n:
            return n
        # keep last path segment if it's a URL/path; strip .git
        short = str(n).rsplit("/", 1)[-1]
        if short.endswith(".git"):
            short = short[:-4]
        return short

    repos_by_app = defaultdict(lambda: defaultdict(set))  # set -> auto-dedupe
    for app_id, tool_type, repo_raw in rows:
        name = friendly(repo_raw)
        if name:
            repos_by_app[app_id][tool_type].add(name)

    # normalize to sorted lists
    normalized = {}
    for app_id, tools in repos_by_app.items():
        normalized[app_id] = {
            str(tool): sorted(list(names))
            for tool, names in tools.items() if names
        }
    return normalized

def build_tree(df, search_term=None):
    """
    Build the application-first hierarchy with:
      - repositories (by tool) per app
      - service instances grouped by environment, de-duplicated
    Returns a list of root nodes.
    """
    df = df.copy()

    # Optional search (prune after build approach simplified here)
    if search_term:
        s = search_term.lower()
        mask = df.apply(lambda row: s in str(row.values).lower(), axis=1)
        matching_apps = set(df[mask]["app_correlation_id"])
        matching_parents = set(df[df["app_correlation_id"].isin(matching_apps)]["application_parent_correlation_id"])
        keep_apps = matching_apps.union(matching_parents)
        df = df[df["app_correlation_id"].isin(keep_apps)]

    # Repositories per app
    app_ids = df["app_correlation_id"].unique().tolist()
    repos_by_app = fetch_repositories(app_ids)

    # Group & dedupe service instances by app and environment (use OrderedDict to preserve first seen)
    insts_map = defaultdict(lambda: defaultdict(OrderedDict))
    for _, row in df.iterrows():
        app_id = row["app_correlation_id"]
        env = row["environment"]
        inst_id = row["instance_correlation_id"]
        inst = {
            "id": inst_id,
            "name": row["instance_name"],
            "install_type": row["install_type"],
            "sysid": row["instance_sysid"],
        }
        # overwrite or keep first; either dedupes by inst_id
        insts_map[app_id][env][inst_id] = inst

    # Convert OrderedDicts -> lists for template consumption
    instances_grouped = {}
    for app_id, env_map in insts_map.items():
        instances_grouped[app_id] = {env: list(env_map[env].values()) for env in env_map}

    # Build node graph
    nodes = {}
    parent_map = {}
    children_map = defaultdict(list)

    for _, row in df.drop_duplicates("app_correlation_id").iterrows():
        app_id = row["app_correlation_id"]
        parent_id = row["application_parent_correlation_id"]

        repo_groups = repos_by_app.get(app_id, {})
        repo_count = sum(len(v) for v in repo_groups.values())
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
            "instances_grouped": instances_grouped.get(app_id, {}),  # env -> [instances...]
            "repositories": repo_groups,    # dict for programmatic use
            "repo_pairs": repo_pairs,       # list[(tool, [repos])]
            "repo_count": repo_count,
            "children": [],
        }

        parent_map[app_id] = parent_id
        if parent_id and parent_id != app_id:
            children_map[parent_id].append(app_id)

    # wire children
    for parent_id, child_ids in children_map.items():
        if parent_id in nodes:
            for child_id in child_ids:
                if child_id in nodes:
                    nodes[parent_id]["children"].append(nodes[child_id])

    # roots
    root_ids = [app_id for app_id in nodes if parent_map.get(app_id) is None or parent_map[app_id] not in nodes]
    root_nodes = [nodes[aid] for aid in sorted(root_ids, key=lambda aid: nodes[aid]["name"])]
    return root_nodes


def _collect_instance_sysids(roots):
    """Collect instance sysids from given root nodes (recursive)."""
    sysids = set()

    def walk(node):
        for env, insts in node.get("instances_grouped", {}).items():
            for inst in insts:
                sid = inst.get("sysid")
                if sid:
                    sysids.add(sid)
        for child in node.get("children", []):
            walk(child)

    for r in roots:
        walk(r)
    return list(sysids)


def fetch_change_requests_for_instances(instance_sysids):
    """
    Batched CR fetch for the given instance_sysids.
    Table: spdw_vwsfaffectedcis (ci_sysid -> task)
    """
    if not instance_sysids:
        return {}

    clean = [str(s) for s in instance_sysids if s not in (None, "", "None")]
    if not clean:
        return {}

    query = """
        SELECT
            ci_sysid,
            task
        FROM public.spdw_vwsfaffectedcis
        WHERE ci_sysid = ANY(%s)
    """
    with connection.cursor() as cursor:
        cursor.execute(query, [clean])
        rows = cursor.fetchall()

    cr_map = defaultdict(list)
    for ci_sysid, task in rows:
        cr_map[ci_sysid].append({"task": task})
    return dict(cr_map)


def _enrich_roots_with_crs(roots, cr_map):
    """Attach 'change_requests' to each instance (by sysid) for the given root nodes."""
    def walk(node):
        for env, insts in node.get("instances_grouped", {}).items():
            for inst in insts:
                inst["change_requests"] = cr_map.get(inst.get("sysid"), [])
        for child in node.get("children", []):
            walk(child)

    for r in roots:
        walk(r)


def application_tree_view(request):
    mode = request.GET.get("mode", "by_si")
    search_term = request.GET.get("search", "").strip()
    page_number = request.GET.get("page", 1)

    # 1) Build full tree (without CRs)
    df = fetch_records(mode)
    tree = build_tree(df, search_term)

    # 2) Paginate roots (25 per page)
    paginator = Paginator(tree, 25)
    page_obj = paginator.get_page(page_number)
    page_roots = page_obj.object_list

    # 3) Fetch CRs only for instances on this page; then enrich
    instance_sysids = _collect_instance_sysids(page_roots)
    cr_map = fetch_change_requests_for_instances(instance_sysids)
    _enrich_roots_with_crs(page_roots, cr_map)

    # 4) Render
    return render(request, "application_tree/tree_view.html", {
        "trees": page_roots,
        "mode": mode,
        "search": search_term,
        "page_obj": page_obj,
    })
