select t.timezone_desc
  from user u, timezone_lu t
 where u.timezone_id = t.timezone_id
  and u.user_id = @uid@