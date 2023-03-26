SELECT {+AVOID_FULL(r)}
       r.resource_id, r.project_id, r.project_phase_id AS phase_id, r.resource_role_id,
       r.create_user, r.create_date, r.modify_user, r.modify_date
FROM resource r
INNER JOIN project p ON r.project_id = p.project_id AND p.project_status_id = @stid@
WHERE r.user_id = @uid@

SELECT {+AVOID_FULL(i2), AVOID_FULL(t)}
       i2.resource_id, i2.value, i2.resource_info_type_id, t.name AS resource_info_type_name
FROM resource r
INNER JOIN project p ON r.project_id = p.project_id AND p.project_status_id = @stid@
INNER JOIN resource_info i2 ON r.resource_id = i2.resource_id
INNER JOIN resource_info_type_lu t ON i2.resource_info_type_id = t.resource_info_type_id and r.user_id = @uid@