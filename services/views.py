from django.shortcuts import render
from django.db import connection
from collections import defaultdict
from .models import ServiceInstanceRecord
from django.core.paginator import Paginator

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
                child_app.application_parent_correlation_id
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
            LEFT JOIN public.vwsfbusinessapplication AS parent_app
              ON child_app.application_parent_correlation_id = parent_app.correlation_id
        """
    else:
        query = """
            SELECT
                fia.lean_control_service_id,
                lpbd.jira_backlog_id,
                bs.service_correlation_id,
                bs.service,
                bac.correlation_id,
                bac.business_application_name,
                si.correlation_id,
                si.it_service_instance,
                si.environment,
                si.install_type,
                bac.application_parent_correlation_id
            FROM public.vwsfitserviceinstance AS si
            JOIN public.lean_control_application AS fia
              ON fia.servicenow_app_id = si.correlation_id
            JOIN public.lean_control_product_backlog_details AS lpbd
              ON lpbd.lct_product_id = fia.lean_control_service_id
             AND lpbd.is_parent = TRUE
            JOIN public.vwsfbusinessapplication AS bac
              ON si.business_application_sysid = bac.business_application_sys_id
            JOIN public.vwsfitbusinessservice AS bs
              ON si.it_business_service_sysid = bs.it_business_service_sysid
        """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return [
        ServiceInstanceRecord(*row[:10], parent_app_id=row[10])
        for row in rows
    ]


def build_app_tree(records, search_term=None):
    apps = defaultdict(lambda: {"instances": [], "children": []})
    roots = []

    for rec in records:
        rec_dict = rec.__dict__
        app_id = rec_dict["app_id"]
        parent_id = rec_dict.get("parent_app_id")

        # Apply search filter if needed
        if search_term:
            searchable = " ".join([
                rec_dict.get("app_name", ""),
                rec_dict.get("instance_name", ""),
                rec_dict.get("service_name", ""),
                rec_dict.get("jira_backlog_id", ""),
                rec_dict.get("environment", ""),
                rec_dict.get("install_type", "")
            ]).lower()
            if search_term.lower() not in searchable:
                continue

        apps[app_id]["app_name"] = rec_dict["app_name"]
        apps[app_id]["instances"].append(rec_dict)

        if parent_id:
            apps[parent_id]["children"].append(app_id)
        else:
            if app_id not in roots:
                roots.append(app_id)

    return {"apps": apps, "roots": roots}


def filter_tree_data(tree_data, search_term):

    if not search_term:
        return tree_data

    search = search_term.lower()
    filtered_apps = {}
    roots = []

    for app_id, app in tree_data["apps"].items():
        matching_instances = [
            inst for inst in app["instances"]
            if any(search in str(value).lower() for key, value in inst.items())
        ]

        if matching_instances or app_id in tree_data["roots"]:
            filtered_apps[app_id] = {
                "app_name": app["app_name"],
                "instances": matching_instances,
                "children": app["children"]
            }

    for root_id in tree_data["roots"]:
        if root_id in filtered_apps:
            roots.append(root_id)

    return {
        "apps": filtered_apps,
        "roots": roots
    }


def service_tree_view(request):
    mode = request.GET.get("mode", "by_si")
    search = request.GET.get("search", "").strip()
    page_number = request.GET.get("page", 1)

    records = fetch_records(mode)
    tree_data = build_app_tree(records)

    if search:
        tree_data = filter_tree_data(tree_data, search)

    # Paginate root apps only
    roots = tree_data["roots"]
    paginator = Paginator(roots, 10)  # 10 roots per page

    page_obj = paginator.get_page(page_number)
    page_roots = page_obj.object_list

    return render(request, "services/service_tree.html", {
        "tree_data": tree_data,
        "mode": mode,
        "search": search,
        "page_obj": page_obj,
        "page_roots": page_roots,
    })
