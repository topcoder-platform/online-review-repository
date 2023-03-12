select unique tdp.project_id as tc_direct_project_id, name as tc_direct_project_name

from tc_direct_project tdp, user_permission_grant upg

where tdp.project_id = upg.resource_id

and upg.user_id = @uid@

and upg.permission_type_id in (0,1,2,3)

and tdp.project_status_id = 1

order by tc_direct_project_name