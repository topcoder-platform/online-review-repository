SELECT p.project_id, p.project_status_id, p.project_category_id,
       p.create_user, p.create_date, p.modify_user, p.modify_date
FROM project p
WHERE p.project_status_id = @stid@

SELECT i.project_id, i.project_info_type_id, i.value
FROM project_info i
INNER JOIN project p ON p.project_id = i.project_id
WHERE p.project_status_id = @stid@