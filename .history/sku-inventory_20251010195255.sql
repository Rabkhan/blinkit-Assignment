/*
SELECT * FROM blinkit_city_map LIMIT 10;

SELECT * FROM all_blinkit_category LIMIT 10;
*/
-- finding inventory_movement

WITH inventory_movement AS (
SELECT
    abc.created_at::date AS date,
    abc.store_id,
    abc.sku_id,
    abc.l1_category_id AS category_id,
    abc.l2_category_id AS sub_category_id,
    abc.sku_name,
    abc.mrp as price,
    abc.selling_price,
    bcm.city_name,
    abc.inventory,
  LEAD(abc.inventory) OVER (PARTITION BY abc.store_id, abc.sku_id ORDER BY abc.created_at) AS next_inventory,
  LEAD(abc.created_at) OVER (PARTITION BY abc.store_id, abc.sku_id ORDER BY abc.created_at) AS next_time

FROM
  all_blinkit_category as abc
  JOIN blinkit_city_map as bcm ON abc.store_id = bcm.store_id
)

SELECT
    im.store_id,
    im.sku_id,
    im.sku_name,
    im.sub_category_id,
    im.category_id,
    im.price,
    im.inventory,
    im.event_datetime,
    im.next_inventory,
    im.next_event_datetime,
    CASE
        WHEN im.next_inventory IS NULL THEN 0
        WHEN im.next_inventory < im.inventory 
        THEN im.inventory - im.next_inventory
        ELSE NULL  -- Will estimate later
        END AS direct_qty_sold

FROM inventory_movement as im;