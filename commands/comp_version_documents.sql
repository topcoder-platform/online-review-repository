SELECT
document_id,
document_name,
url,
document_type_id
FROM comp_documentation
WHERE
comp_vers_id = @cv@