from django.shortcuts import render
from django.db import connection
from collections import defaultdict
from .models import ServiceInstanceRecord
from django.core.paginator import Paginator
import pandas as pd


def fetch_records(mode):
    if mode == "by_ts":
        query = """
            SELECT
                lca.lean_control_service_id,
                lpbd.jira_backlog_id,
                bs.service_correlation_id,
                bs.service,
                child_app.correlation_id,
                child_app.business_application_name,
                si.correlation_id,
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
    else:
        query = """
            SELECT
                fia.lean_control_service_id,
                lpbd_dedup.jira_backlog_id,
                bs.service_correlation_id,
                bs.service,
                bac.correlation_id,
                bac.business_application_name,
                si.correlation_id,
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
        """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    columns = [
        "lean_control_service_id", "jira_backlog_id", "service_id", "service_name",
        "app_id", "app_name", "instance_id", "instance_name", "environment", "install_type",
        "parent_app_id", "application_type", "application_tier", "architecture_type"
    ]
    return pd.DataFrame(rows, columns=columns)


def build_service_app_tree(df, search_term=None):
    if search_term:
        search_term = search_term.lower()
        df = df[df.apply(lambda row: search_term in str(row.values).lower(), axis=1)]

    services = {}
    roots = []

    for lcs_id, service_df in df.groupby("lean_control_service_id"):
        apps = defaultdict(lambda: {
            "app_name": None,
            "instances": [],
            "children": [],
            "parent": None  # used by your template
        })

        for _, row in service_df.iterrows():
            app_id = row["app_id"]
            parent_id = row["parent_app_id"]

            app = apps[app_id]
            app["app_name"] = row["app_name"]
            app["instances"].append(row.to_dict())
            app["parent"] = parent_id

            if parent_id:
                apps[parent_id]["children"].append(app_id)

        services[lcs_id] = {"apps": dict(apps)}
        roots.append(lcs_id)

    return {"services": services, "roots": roots}


def service_tree_view(request):
    mode = request.GET.get("mode", "by_si")
    search = request.GET.get("search", "").strip()
    page_number = request.GET.get("page", 1)

    df = fetch_records(mode)
    tree_data = build_service_app_tree(df, search_term=search)

    roots = tree_data["roots"]
    paginator = Paginator(roots, 10)
    page_obj = paginator.get_page(page_number)
    page_roots = page_obj.object_list

    paginated_services = {sid: tree_data["services"][sid] for sid in page_roots}

    return render(request, "services/service_tree.html", {
        "lcs_services": paginated_services,
        "mode": mode,
        "search": search,
        "page_obj": page_obj,
        "page_roots": page_roots,
    })
