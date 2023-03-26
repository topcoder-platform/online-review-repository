SELECT DISTINCT
       p.project_id, p.project_status_id, p.project_category_id,
       p.create_user, p.create_date, p.modify_user, p.modify_date
FROM project p
INNER JOIN resource r ON r.project_id=p.project_id and r.user_id=@uid@
WHERE p.project_status_id = 1

SELECT DISTINCT
pi.project_id, pi.project_info_type_id, pi.value
FROM project_info pi
INNER JOIN project p ON p.project_id = pi.project_id
INNER JOIN resource r ON r.project_id=p.project_id and r.user_id=@uid@
WHERE p.project_status_id = 1