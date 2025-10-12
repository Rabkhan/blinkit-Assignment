/*
Dcluttr Data Analyst SQL Assignment


Candidate: Abdurrab Khan

Tools Used: PostgreSQL, VS Code, pgAdmin4, psql CLI
Database Setup:
   1. Created a new database in pgAdmin4.
   2. Imported the three provided CSVs using psql \copy command.
   3. Validated table schemas against provided documentation.

Objective:
   Create a derived table `blinkit_city_insights` that integrates
   Blinkit SKU, category, and city data to estimate:
     - Estimated quantity sold per SKU per city per day
     - Price modes (most frequent selling/MRP)
     - Store-level availability metrics
     - Weighted on-shelf availability (OSA)

Approach Summary:
   1. Calculated inventory movement using LEAD() to compare sequential inventory snapshots.
   2. Derived direct sales from inventory drops.
   3. Estimated restock intervals using historical average sales.
   4. Joined city, category, and pricing data.
   5. Computed aggregated metrics including revenue, availability, and discount.
   6. Added extra insight column: `in_stock_store_count`
      → actual count of stores where SKU is available.

Output:
   Final table: `blinkit_city_insights`
   Output exported to CSV for submission.

*/

CREATE TABLE IF NOT EXISTS blinkit_city_insights AS

-- finding inventory_movement
WITH inventory_movement AS
(
 SELECT
    abc.created_at::timestamp AS event_time, -- precise timestamp
    abc.created_at::date AS event_date, -- date for aggregation
    abc.store_id,
    abc.sku_id,
    abc.l1_category_id  AS category_id,
    abc.l2_category_id  AS sub_category_id,
    abc.sku_name,
    abc.brand_id, 
    abc.brand,    
    abc.image_url,
    abc.mrp AS mrp,
    abc.selling_price,
    bcm.city_name,
    abc.inventory,
     -- next inventory values to track changes over time
    LEAD(abc.inventory) OVER ( PARTITION BY abc.store_id, abc.sku_id ORDER BY abc.created_at ) AS next_inventory,
    LEAD(abc.created_at) OVER ( PARTITION BY abc.store_id, abc.sku_id ORDER BY abc.created_at) AS next_event_time
  FROM
    all_blinkit_category as abc  
    JOIN blinkit_city_map bcm ON abc.store_id = bcm.store_id
),

-- figure out direct sales when inventory drops
sales_calc AS 
(
  SELECT im.*,
    CASE 
        WHEN im.next_inventory IS NULL THEN NULL  -- last record, can't calculate
        WHEN im.next_inventory < im.inventory THEN (im.inventory - im.next_inventory)  -- sold some units
        ELSE NULL  -- restock happened or no change
    END AS direct_qty_sold

  FROM inventory_movement im
),

-- need avg sales per sku and store for when restocks happen
avg_historical_sales AS 
(
SELECT 
    store_id,
    sku_id,
    AVG(direct_qty_sold) AS avg_sold  -- average of previous actual sales
  
  FROM sales_calc
  WHERE direct_qty_sold IS NOT NULL  -- only count real sales, not restocks
  GROUP BY store_id, sku_id
),

-- now estimate qty sold including restock scenarios
estimated_qty AS (
  SELECT
    sc.event_date,
    sc.event_time,
    sc.store_id,
    sc.sku_id,
    sc.category_id,
    sc.sub_category_id,
    sc.sku_name,
    sc.brand_id,
    sc.brand,
    sc.image_url,
    sc.city_name,
    sc.mrp,
    sc.selling_price,
    sc.inventory,
    sc.next_inventory,
    CASE
      WHEN sc.direct_qty_sold IS NOT NULL THEN sc.direct_qty_sold  -- actual sales
      WHEN sc.next_inventory > sc.inventory THEN COALESCE(ahs.avg_sold, 0)  -- restock: use avg
      ELSE 0  -- no next record or equal inventory
    END AS est_qty_sold_per_record
  
  FROM sales_calc sc
  LEFT JOIN avg_historical_sales ahs ON sc.store_id = ahs.store_id AND sc.sku_id = ahs.sku_id
),

-- checking in_stock status at store level for On shelf availability  calculations
stock_status AS 
(
    SELECT
        abc.created_at::date AS event_date,
        abc.sku_id,
        abc.store_id,
        bcm.city_name,
        CASE 
            WHEN abc.inventory > 0 THEN 1 ELSE 0 END AS is_in_stock
        
    FROM all_blinkit_category abc
    JOIN blinkit_city_map bcm ON abc.store_id = bcm.store_id
),

-- get most common prices using window function approach
price_modes AS 
(
    SELECT
        event_date,
        sku_id,
        city_name,
        mrp,
        selling_price,
        COUNT(*) as price_freq,
        ROW_NUMBER() OVER ( PARTITION BY event_date, sku_id, city_name ORDER BY COUNT(*) DESC, mrp DESC) as mrp_rank,
        ROW_NUMBER() OVER ( PARTITION BY event_date, sku_id, city_name  ORDER BY COUNT(*) DESC, selling_price DESC) as sp_rank
    
    FROM estimated_qty
    GROUP BY event_date, sku_id, city_name, mrp, selling_price
),

-- rolling up to city level now
city_level_agg AS (
  SELECT
    eq.event_date,
    eq.sku_id,
    eq.city_name,
    eq.category_id,
    eq.sub_category_id,
    eq.sku_name,
    eq.brand_id,
    eq.brand,
    eq.image_url,
    SUM(eq.est_qty_sold_per_record) AS est_qty_sold, -- total estimated sales across all stores in city
    -- getting the most common prices from price_modes
    MAX(CASE WHEN pm_mrp.mrp_rank = 1 THEN pm_mrp.mrp END) AS mrp_mode,
    MAX(CASE WHEN pm_sp.sp_rank = 1 THEN pm_sp.selling_price END) AS sp_mode,
   
    COUNT(DISTINCT eq.store_id) AS listed_ds_count,  -- count how many stores carry this sku (listed or not)
   
    COUNT(DISTINCT CASE WHEN ss.is_in_stock = 1 THEN ss.store_id END) AS in_stock_store_count  -- count distinct stores where it's in stock
  
  FROM estimated_qty eq

  LEFT JOIN stock_status ss ON eq.event_date = ss.event_date 
    AND eq.sku_id = ss.sku_id 
    AND eq.store_id = ss.store_id
    AND eq.city_name = ss.city_name

  LEFT JOIN price_modes pm_mrp ON eq.event_date = pm_mrp.event_date
    AND eq.sku_id = pm_mrp.sku_id
    AND eq.city_name = pm_mrp.city_name
    AND pm_mrp.mrp_rank = 1

  LEFT JOIN price_modes pm_sp ON eq.event_date = pm_sp.event_date
    AND eq.sku_id = pm_sp.sku_id
    AND eq.city_name = pm_sp.city_name
    AND pm_sp.sp_rank = 1
  
  GROUP BY 
    eq.event_date,
    eq.sku_id,
    eq.city_name,
    eq.category_id,
    eq.sub_category_id,
    eq.sku_name,
    eq.brand_id,
    eq.brand,
    eq.image_url
),

-- total darkstores per city per date (denominator for OSA)
total_stores_per_city AS 
(
    SELECT
        abc.created_at::date AS event_date,
        bcm.city_name,
        COUNT(DISTINCT abc.store_id) AS total_ds_count
    
    FROM all_blinkit_category abc
    JOIN blinkit_city_map bcm ON abc.store_id = bcm.store_id
    GROUP BY abc.created_at::date, bcm.city_name
)


-- final output table 
SELECT
    cla.event_date AS date,
    cla.brand_id,
    cla.brand,
    cla.image_url,
    cla.city_name,
    cla.sku_id,
    cla.sku_name,
    cla.category_id,
    bc.l1_category AS category_name,
    cla.sub_category_id,
    bc.l2_category AS sub_category_name,
    ROUND(cla.est_qty_sold::NUMERIC) AS est_qty_sold, -- to make the quantity in whole numbers
    ROUND((cla.est_qty_sold * cla.sp_mode)::NUMERIC, 2) AS est_sales_sp,  -- revenue on selling price
    ROUND((cla.est_qty_sold * cla.mrp_mode)::NUMERIC, 2) AS est_sales_mrp,  -- revenue on mrp
    cla.listed_ds_count,
    tsc.total_ds_count AS ds_count,
    cla.in_stock_store_count,  -- actual count of stores where sku is in stock
    -- weighted on shelf availability across all stores
    ROUND ((CASE WHEN tsc.total_ds_count > 0 THEN cla.in_stock_store_count::FLOAT / tsc.total_ds_count ELSE 0 END)::NUMERIC,2 ) AS wt_osa, 

  -- on shelf availability only for stores that list this sku
   ROUND((CASE WHEN cla.listed_ds_count > 0 THEN cla.in_stock_store_count::FLOAT / cla.listed_ds_count ELSE 0 END)::NUMERIC,2) AS wt_osa_ls, 


  cla.mrp_mode AS mrp,
  cla.sp_mode AS sp,

  ROUND((CASE WHEN cla.mrp_mode > 0 THEN (cla.mrp_mode - cla.sp_mode)::FLOAT / cla.mrp_mode ELSE 0 END)::NUMERIC,2) AS discount   -- discount percentage

FROM city_level_agg cla

LEFT JOIN total_stores_per_city tsc ON cla.event_date = tsc.event_date AND cla.city_name = tsc.city_name
LEFT JOIN blinkit_categories bc  ON cla.sub_category_id = bc.l2_category_id

WHERE cla.est_qty_sold > 0  -- skip records with zero sales
ORDER BY cla.event_date, cla.city_name, cla.sku_id
-- LIMIT 100;



/* 
 ADDITIONAL ANALYSIS & INSIGHTS SECTION

These queries were executed on the derived table `blinkit_city_insights` 
to extract key business insights beyond the base requirement.

Each query below provides a different commercial or operational perspective 
that helps in understanding product demand, store performance, and pricing behavior.
*/

/*
-- Top 10 SKUs across all cities
SELECT sku_name, brand, SUM(est_qty_sold) AS total_sold
FROM blinkit_city_insights
GROUP BY sku_name, brand
ORDER BY total_sold DESC
LIMIT 50;


-- Top 5 SKUs per city
SELECT city_name, sku_name, SUM(est_qty_sold) AS total_sold
FROM blinkit_city_insights
GROUP BY city_name, sku_name
ORDER BY city_name, total_sold DESC
LIMIT 50;


-- SKUs that run out of stock most frequently
SELECT sku_name, city_name,
       ROUND(AVG(1 - wt_osa_ls), 2) AS avg_out_of_stock_ratio,
       SUM(est_qty_sold) AS total_sold
FROM blinkit_city_insights
GROUP BY sku_name, city_name
HAVING SUM(est_qty_sold) > 0
ORDER BY avg_out_of_stock_ratio DESC
LIMIT 50;


-- top SKUs or brands contributing most to sales value
SELECT brand, SUM(est_sales_sp) AS total_revenue
FROM blinkit_city_insights
GROUP BY brand
ORDER BY total_revenue DESC
LIMIT 10;

--cities contribute most to total sales or have highest average basket value

SELECT city_name,
       SUM(est_sales_sp) AS total_sales,
       SUM(est_qty_sold) AS total_units,
       ROUND(SUM(est_sales_sp)/SUM(est_qty_sold),2) AS avg_unit_price
FROM blinkit_city_insights
GROUP BY city_name
ORDER BY total_sales DESC
LIMIT 10;


-- most popular categories/subcategories overall and in specific cities.

SELECT category_name, sub_category_name, SUM(est_sales_sp) AS total_sales
FROM blinkit_city_insights
GROUP BY category_name, sub_category_name
ORDER BY total_sales DESC;


-- Test whether deeper discounts actually increase sales.

SELECT ROUND(discount,2) AS discount_band,
       ROUND(AVG(est_qty_sold),2) AS avg_qty_sold
FROM blinkit_city_insights
GROUP BY ROUND(discount,2)
ORDER BY discount_band;

*/