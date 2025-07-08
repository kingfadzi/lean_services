from django.shortcuts import render
from django.core.paginator import Paginator
from collections import defaultdict
from django.db import connection
from .models import ServiceInstanceRecord


def fetch_service_records():
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
            si.install_type
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

    return [ServiceInstanceRecord(*row) for row in rows]


def service_tree_view(request):
    query = request.GET.get("q", "").strip().lower()
    page_number = request.GET.get("page", 1)

    records = fetch_service_records()

    tree = defaultdict(list)
    for rec in records:
        tree[rec.app_id].append(rec.__dict__)

    # Apply search filter
    if query:
        tree = {
            k: v for k, v in tree.items()
            if query in k.lower() or query in v[0].get("app_name", "").lower()
        }

    # Paginate app groups
    app_items = list(tree.items())
    paginator = Paginator(app_items, 25)
    page_obj = paginator.get_page(page_number)

    return render(request, "services/service_tree.html", {
        "page_obj": page_obj,
        "query": query,
    })