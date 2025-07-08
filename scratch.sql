SELECT DISTINCT
    parent.correlation_id AS parent_app_id,
    parent.business_application_name AS parent_app_name
FROM public.vwsfbusinessapplication AS parent
         JOIN public.vwsfbusinessapplication AS child
              ON child.application_parent_correlation_id = parent.correlation_id
WHERE parent.correlation_id IN (
    SELECT DISTINCT
        si.business_application_sysid
    FROM public.vwsfitserviceinstance AS si
             JOIN public.lean_control_application AS lca
                  ON lca.servicenow_app_id = si.correlation_id
             JOIN public.lean_control_product_backlog_details AS lpbd
                  ON lpbd.lct_product_id = lca.lean_control_service_id
                      AND lpbd.is_parent = TRUE
)
ORDER BY parent.business_application_name;
