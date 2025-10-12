SELECT *
FROM blinkit_city_map
LIMIT 5;


SELECT
  abc.store_id,
  abc.sku_id,
  abc.created_at::date AS date,
  bcm.city_name,
  abc.inventory,
  abc.selling_price,
  abc.mrp,
  LEAD(abc.inventory) OVER (PARTITION BY abc.store_id, abc.sku_id ORDER BY abc.created_at) AS next_inventory,
  LEAD(abc.created_at) OVER (PARTITION BY abc.store_id, abc.sku_id ORDER BY abc.created_at) AS next_time
FROM
  all_blinkit_category abc
  JOIN blinkit_city_map bcm ON abc.store_id = bcm.store_id
LIMIT 50;


WITH inventory_movement AS (
  SELECT
    abc.store_id,
    abc.sku_id,
    abc.created_at::date AS date,
    bcm.city_name,
    abc.inventory,
    abc.selling_price,
    abc.mrp,
    LEAD(abc.inventory) OVER (
      PARTITION BY abc.store_id, abc.sku_id 
      ORDER BY abc.created_at
    ) AS next_inventory,
    LEAD(abc.created_at) OVER (
      PARTITION BY abc.store_id, abc.sku_id 
      ORDER BY abc.created_at
    ) AS next_time
  FROM
    all_blinkit_category abc
    JOIN blinkit_city_map bcm ON abc.store_id = bcm.store_id
)
SELECT
  store_id,
  sku_id,
  date,
  city_name,
  inventory,
  selling_price,
  mrp,
  next_inventory,
  next_time,
  CASE 
    WHEN next_inventory IS NOT NULL AND inventory > next_inventory 
      THEN inventory - next_inventory
    ELSE 0
  END AS est_qty_sold
FROM inventory_movement
LIMIT 50;
