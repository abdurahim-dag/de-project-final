COPY ST23051601__STAGING.%(table_name)s(%(column_names)s) FROM LOCAL STDIN DELIMITER ',' ENCLOSED BY '"' REJECTED DATA AS TABLE ST23051601__STAGING.%(table_name)s_rej;