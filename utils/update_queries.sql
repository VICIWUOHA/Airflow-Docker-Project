-- Update for existing products with new prices
WITH updates_table as(
        SELECT stg.* 
        FROM 
        competitor_prices_stg stg
        LEFT JOIN competitor_prices prod
        ON stg.product_id = prod.product_id
        WHERE stg.price <> prod.current_price)
UPDATE competitor_prices cp
SET 
    last_seen_at_old_price = date_extracted,
    date_extracted = upd.date,
    old_price = current_price,
    current_price = upd.price
FROM updates_table AS upd
where cp.product_id = upd.product_id
;
-- Updates for new products added to vendor
-- Assumption is that this product_id has not been seen in our db before
WITH new_products_table as (
		SELECT stg.* 
		FROM competitor_prices_stg stg
		LEFT JOIN competitor_prices prod
		ON stg.product_id = prod.product_id
		WHERE prod.product_id is NULL)     
    INSERT INTO competitor_prices (date_extracted,business,category,product_id,product_name,current_price)
    SELECT  np.date , np.business, np.category, np.product_id,np.product_name,np.price
    FROM new_products_table np
    ON CONFLICT (product_id)
        DO NOTHING 
;
-- At the end of the transaction, Update all dates in the competitor prices db
-- from the stg table except the retired products which would not be available on stg table
UPDATE competitor_prices cp
SET date_extracted = stg.date
FROM competitor_prices_stg AS stg
WHERE cp.product_id = stg.product_id

