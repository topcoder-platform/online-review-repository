SELECT
  su.password, u.status
FROM
  security_user su, user u
WHERE
  su.login_id = @uid@
 and u.user_id = su.login_id