CREATE TABLE all_blinkit_category
(
    created_at    timestamptz    NOT NULL,
    l1_category_id bigint,
    l2_category_id bigint,
    store_id      bigint         NOT NULL,
    sku_id        bigint         NOT NULL,
    sku_name      text,
    selling_price numeric(10,2),
    mrp           numeric(10,2),
    inventory     integer,
    image_url     text,
    brand_id      bigint,
    brand         text,
    unit          text,
    PRIMARY KEY (created_at, sku_id, store_id)
); 