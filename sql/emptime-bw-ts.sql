WITH in_out AS (
    SELECT 
        employee_id,
        action,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY employee_id, action ORDER BY created_at) AS rn
    FROM employee_movement
),

paired AS (
    SELECT 
        i.employee_id,
        i.created_at AS in_time,
        o.created_at AS out_time
    FROM in_out i
    JOIN in_out o 
        ON i.employee_id = o.employee_id 
       AND i.rn = o.rn 
       AND i.action = 'In' 
       AND o.action = 'Out'
)

SELECT 
    employee_id,
    ROUND(SUM(
        CASE 
            WHEN out_time > TIMESTAMP '2025-11-11 09:00:00'
             AND in_time < TIMESTAMP '2025-11-11 17:00:00'
            THEN 
                EXTRACT(HOUR FROM (LEAST(out_time, TIMESTAMP '2025-11-11 17:00:00') - 
                                    GREATEST(in_time, TIMESTAMP '2025-11-11 09:00:00')))
              + EXTRACT(MINUTE FROM (LEAST(out_time, TIMESTAMP '2025-11-11 17:00:00') - 
                                      GREATEST(in_time, TIMESTAMP '2025-11-11 09:00:00'))) / 60.0
              + EXTRACT(SECOND FROM (LEAST(out_time, TIMESTAMP '2025-11-11 17:00:00') - 
                                      GREATEST(in_time, TIMESTAMP '2025-11-11 09:00:00'))) / 3600.0
            ELSE 0
        END
    ), 2) AS hours_in_window
FROM paired
GROUP BY employee_id
ORDER BY employee_id;
