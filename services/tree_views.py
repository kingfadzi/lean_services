from django.shortcuts import render
from django.db import connection
from django.core.paginator import Paginator
from collections import defaultdict, OrderedDict
import pandas as pd


def fetch_records(mode):
    """
    Base dataset (apps, services, instances) with extra business app fields
    for filtering/display (tier, type, owning_transaction_cycle, operational_status).
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
                child_app.owning_transaction_cycle,
                child_app.operational_status,
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
                bac.owning_transaction_cycle,
                bac.operational_status,
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
        "lean_control_service_id", "jira_backlog_id",
        "service_id", "service_name",
        "app_correlation_id", "app_name",
        "instance_correlation_id", "instance_name",
        "environment", "install_type",
        "application_parent_correlation_id",
        "application_type", "application_tier", "architecture_type",
        "owning_transaction_cycle", "operational_status",
        "instance_sysid",
    ]

    df = pd.DataFrame(rows, columns=columns)
    # Normalize dtypes for categorical fields
    for col in ["application_type", "application_tier", "architecture_type",
                "owning_transaction_cycle", "operational_status",
                "environment", "install_type"]:
        if col in df.columns:
            df[col] = df[col].astype("object")
    # Stable sort by app name
    df.sort_values("app_name", inplace=True, kind="mergesort")
    return df


def fetch_repositories(app_ids):
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
        short = str(n).rsplit("/", 1)[-1]
        if short.endswith(".git"):
            short = short[:-4]
        return short

    repos_by_app = defaultdict(lambda: defaultdict(set))
    for app_id, tool_type, repo_raw in rows:
        name = friendly(repo_raw)
        if name:
            repos_by_app[app_id][tool_type].add(name)

    return {
        app_id: {str(tool): sorted(list(names)) for tool, names in tools.items() if names}
        for app_id, tools in repos_by_app.items()
    }


def build_tree(df, search_term=None):
    """
    Build the application-centric tree from a filtered dataframe.
    Instances are grouped by environment.
    Optional global 'search_term' trims df to apps that match (keeping parents).
    """
    df = df.copy()

    # Global search filter (case-insensitive) across row values
    if search_term:
        s = search_term.lower()
        mask = df.apply(lambda row: s in str(row.values).lower(), axis=1)
        matching_apps = set(df[mask]["app_correlation_id"])
        matching_parents = set(
            df[df["app_correlation_id"].isin(matching_apps)]["application_parent_correlation_id"]
        )
        keep_apps = matching_apps.union(matching_parents)
        df = df[df["app_correlation_id"].isin(keep_apps)]

    # Fetch repos once for all apps in current df
    app_ids = df["app_correlation_id"].unique().tolist()
    repos_by_app = fetch_repositories(app_ids)

    # Group instances by environment per app
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
        if app_id is not None and env is not None and inst_id is not None:
            insts_map[app_id][env][inst_id] = inst

    instances_grouped = {
        app_id: {env: list(env_map[env].values()) for env in env_map}
        for app_id, env_map in insts_map.items()
    }

    # Build nodes and parent/child maps
    nodes, parent_map, children_map = {}, {}, defaultdict(list)
    for _, row in df.drop_duplicates("app_correlation_id").iterrows():
        app_id = row["app_correlation_id"]
        parent_id = row["application_parent_correlation_id"]
        repo_groups = repos_by_app.get(app_id, {})
        repo_count = sum(len(v) for v in repo_groups.values())
        repo_pairs = sorted(repo_groups.items(), key=lambda t: (t[0] or "").lower())

        nodes[app_id] = {
            "id": app_id,
            "name": row["app_name"],
            "type": row["application_type"],
            "tier": row["application_tier"],
            "architecture": row["architecture_type"],
            "owning_transaction_cycle": row.get("owning_transaction_cycle"),
            "operational_status": row.get("operational_status"),
            "lean_control_id": row["lean_control_service_id"],
            "jira_backlog_id": row["jira_backlog_id"],
            "service_name": row["service_name"],
            "service_id": row["service_id"],
            "instances_grouped": instances_grouped.get(app_id, {}),
            "repositories": repo_groups,
            "repo_pairs": repo_pairs,
            "repo_count": repo_count,
            "children": [],
        }
        parent_map[app_id] = parent_id
        if parent_id and parent_id != app_id:
            children_map[parent_id].append(app_id)

    # Attach children
    for parent_id, child_ids in children_map.items():
        if parent_id in nodes:
            for child_id in child_ids:
                if child_id in nodes:
                    nodes[parent_id]["children"].append(nodes[child_id])

    # Roots = apps whose parent is None or not in current nodes
    root_ids = [
        aid for aid in nodes
        if parent_map.get(aid) is None or parent_map.get(aid) not in nodes
    ]
    roots = [nodes[aid] for aid in sorted(root_ids, key=lambda aid: nodes[aid]["name"])]
    return roots


def _collect_instance_sysids(roots):
    sysids = set()

    def walk(node):
        for _, insts in node.get("instances_grouped", {}).items():
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
    if not instance_sysids:
        return {}
    clean = [str(s) for s in instance_sysids if s not in (None, "", "None")]
    if not clean:
        return {}

    query = """
        SELECT ci_sysid, task
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
    def walk(node):
        for _, insts in node.get("instances_grouped", {}).items():
            for inst in insts:
                inst["change_requests"] = cr_map.get(inst.get("sysid"), [])
        for child in node.get("children", []):
            walk(child)
    for r in roots:
        walk(r)


def _make_filter_options(df):
    def opts(col):
        if col not in df.columns:
            return []
        vals = (
            df[col]
            .dropna()
            .astype(str)
            .map(str.strip)
            .replace({"": None, "None": None})
            .dropna()
            .unique()
            .tolist()
        )
        return sorted(vals, key=lambda v: v.lower())

    return {
        "tiers": opts("application_tier"),
        "app_types": opts("application_type"),
        "transaction_cycles": opts("owning_transaction_cycle"),
        "operational_statuses": opts("operational_status"),
        "environments": opts("environment"),
        "install_types": opts("install_type"),
        # Uncomment if you want long dropdowns for services:
        # "service_names": opts("service_name"),
        # "service_ids": opts("service_id"),
    }


def _apply_filters(
        df,
        tier=None,
        app_type=None,
        txn_cycle=None,
        op_status=None,
        service_name=None,
        service_id=None,
        environment=None,
        install_type=None,
):
    """
    Apply column-level filters. service_name is case-insensitive partial match.
    Others are exact matches.
    """
    filt = df.copy()

    if tier:
        filt = filt[filt["application_tier"].astype(str) == str(tier)]

    if app_type:
        filt = filt[filt["application_type"].astype(str) == str(app_type)]

    if txn_cycle and "owning_transaction_cycle" in filt.columns:
        filt = filt[filt["owning_transaction_cycle"].astype(str) == str(txn_cycle)]

    if op_status and "operational_status" in filt.columns:
        filt = filt[filt["operational_status"].astype(str) == str(op_status)]

    if service_name:
        s = service_name.strip().lower()
        filt = filt[filt["service_name"].astype(str).str.lower().str.contains(s, na=False)]

    if service_id:
        filt = filt[filt["service_id"].astype(str) == str(service_id)]

    if environment:
        filt = filt[filt["environment"].astype(str) == str(environment)]

    if install_type:
        filt = filt[filt["install_type"].astype(str) == str(install_type)]

    return filt


def application_tree_view(request):
    # --- Query params ---
    mode = request.GET.get("mode", "by_si")
    search_term = request.GET.get("search", "").strip()
    page_number = request.GET.get("page", 1)

    # Application-level
    selected_tier = request.GET.get("tier", "").strip()
    selected_app_type = request.GET.get("app_type", "").strip()
    selected_txn_cycle = request.GET.get("transaction_cycle", "").strip()
    selected_op_status = request.GET.get("operational_status", "").strip()

    # Service-level
    selected_service_name = request.GET.get("service_name", "").strip()
    selected_service_id = request.GET.get("service_id", "").strip()

    # Instance-level
    selected_environment = request.GET.get("environment", "").strip()
    selected_install_type = request.GET.get("install_type", "").strip()

    # Children filter (both|with|without)
    children_filter = request.GET.get("children", "both").strip().lower()

    # --- Base dataset & filter options ---
    base_df = fetch_records(mode)
    filter_options = _make_filter_options(base_df)

    # --- Apply column-level filters ---
    df = _apply_filters(
        base_df,
        tier=selected_tier or None,
        app_type=selected_app_type or None,
        txn_cycle=selected_txn_cycle or None,
        op_status=selected_op_status or None,
        service_name=selected_service_name or None,
        service_id=selected_service_id or None,
        environment=selected_environment or None,
        install_type=selected_install_type or None,
    )

    # --- Build roots after column-level filters + global search ---
    all_roots = build_tree(df, search_term)

    # --- Children filter (no split view; single unified list) ---
    if children_filter == "with":
        roots_for_paging = [r for r in all_roots if r.get("children")]
    elif children_filter == "without":
        roots_for_paging = [r for r in all_roots if not r.get("children")]
    else:
        roots_for_paging = all_roots

    # --- Pagination (25 per page) ---
    paginator = Paginator(roots_for_paging, 25)
    page_obj = paginator.get_page(page_number)
    page_roots = page_obj.object_list

    # --- Enrich instances on the current page with CRs ---
    instance_sysids = _collect_instance_sysids(page_roots)
    cr_map = fetch_change_requests_for_instances(instance_sysids)
    _enrich_roots_with_crs(page_roots, cr_map)

    return render(request, "application_tree/tree_view.html", {
        "trees": page_roots,
        "mode": mode,
        "search": search_term,
        "page_obj": page_obj,
        "children_filter": children_filter,
        "filter_options": filter_options,
        "selected_tier": selected_tier,
        "selected_app_type": selected_app_type,
        "selected_txn_cycle": selected_txn_cycle,
        "selected_op_status": selected_op_status,
        "selected_service_name": selected_service_name,
        "selected_service_id": selected_service_id,
        "selected_environment": selected_environment,
        "selected_install_type": selected_install_type,
    })
