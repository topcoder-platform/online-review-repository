SELECT
cv.comp_vers_id as version_id
FROM
comp_versions cv
WHERE cv.component_id = @cd@
AND cv.version = @vid@