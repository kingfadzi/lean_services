SELECT DISTINCT
    parent.correlation_id AS parent_app_id,
    parent.business_application_name AS parent_app_name
FROM public.vwsfbusinessapplication AS parent
         JOIN public.vwsfbusinessapplication AS child
              ON child.application_parent_correlation_id = parent.correlation_id
ORDER BY parent.business_application_name;