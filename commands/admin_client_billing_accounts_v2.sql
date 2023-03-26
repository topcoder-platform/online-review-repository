SELECT tdp.project_id as direct_project_id, tdp.name as direct_project_name, p.project_id as billing_account_id, p.name as billing_account_name,
c.client_id as client_id, c.name as client_name, p.active as active, p.end_date as end_date
FROM tc_direct_project tdp, corporate_oltp:direct_project_account dpa, tt_project p, tt_client_project cp, tt_client c 
WHERE tdp.project_id = dpa.project_id 
AND dpa.billing_account_id = p.project_id 
AND p.project_id = cp.project_id
AND cp.client_id = c.client_id