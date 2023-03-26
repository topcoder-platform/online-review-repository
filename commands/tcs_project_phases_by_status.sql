SELECT pp.project_phase_id, pp.project_id, pp.fixed_start_time,
       pp.scheduled_start_time, pp.scheduled_end_time, pp.actual_start_time, pp.actual_end_time, pp.duration,
       pp.phase_type_id, pp.phase_status_id,
       d.dependency_phase_id, d.dependent_phase_id, d.dependency_start, d.dependent_start, d.lag_time
FROM project_phase pp
INNER JOIN project p ON pp.project_id = p.project_id
LEFT JOIN phase_dependency d ON pp.project_phase_id = d.dependent_phase_id
WHERE p.project_status_id = @stid@
ORDER BY pp.project_id,  pp.scheduled_start_time