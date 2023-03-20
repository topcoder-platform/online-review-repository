SELECT
    i.image_id as image_id,
    i.file_name as file_name,
    i.height as height,
    i.image_type_id as image_type,
    i.link as link,
    i.modify_date as modify_date,
    i.original_file_name as original_file_name,
    (select path from path p where p.path_id = i.path_id) as image_path,
    i.width as width
FROM
    coder_image_xref mi
 INNER JOIN
    image i
        on mi.image_id=i.image_id
WHERE
    mi.coder_id=@cr@
AND mi.display_flag=1