WITH in_out AS 
(
    SELECT 
        employee_id,
        action,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY employee_id, action ORDER BY created_at) AS rn
    FROM employee_movement
),

paired AS
(
SELECT 
    i.employee_id,
	i.action, o.action,
    i.created_at AS in_time,
    o.created_at AS out_time
FROM in_out i
JOIN in_out o 
    ON i.employee_id = o.employee_id 
   AND i.rn = o.rn 
   AND i.action = 'In' 
   AND o.action = 'Out'
ORDER BY i.employee_id, in_time
)

SELECT 
    employee_id,
    ROUND(SUM(EXTRACT(HOUR FROM out_time - in_time)
        + EXTRACT(MINUTE FROM out_time - in_time) / 60.0
        + EXTRACT(SECOND FROM out_time - in_time) / 3600.0
    ), 2) AS total_hours
FROM paired
GROUP BY employee_id
ORDER BY employee_id;

