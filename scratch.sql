-- For by_si-linked apps with children
SELECT DISTINCT parent.correlation_id AS parent_app_id
FROM public.vwsfbusinessapplication AS parent
         JOIN public.vwsfbusinessapplication AS child
              ON child.application_parent_correlation_id = parent.correlation_id
WHERE parent.correlation_id IN (
    SELECT bac.correlation_id
    FROM public.vwsfitserviceinstance AS si
             JOIN public.lean_control_application AS lca
                  ON lca.servicenow_app_id = si.correlation_id
             JOIN public.lean_control_product_backlog_details AS lpbd
                  ON lpbd.lct_product_id = lca.lean_control_service_id
                      AND lpbd.is_parent = TRUE
             JOIN public.vwsfbusinessapplication AS bac
                  ON si.business_application_sysid = bac.business_application_sys_id
)


-- For by_ts-linked apps with children
SELECT DISTINCT parent.correlation_id AS parent_app_id
FROM public.vwsfbusinessapplication AS parent
         JOIN public.vwsfbusinessapplication AS child
              ON child.application_parent_correlation_id = parent.correlation_id
WHERE parent.correlation_id IN (
    SELECT parent_app.correlation_id
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
    WHERE parent_app.correlation_id IS NOT NULL
)
