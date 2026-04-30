-- Calculate total customers to establish the modulo boundary
WITH customer_count AS (
    SELECT count(*) AS cnt FROM lakehouse.cdc.silver_customers
)
SELECT
    t.trip_id,
    t.PULocationID,
    t.fare_amount,
    t.trip_distance,
    t.tpep_pickup_datetime,
    c.id AS simulated_customer_id,
    c.name AS customer_name,
    c.country AS customer_country
FROM lakehouse.taxi.silver_trips t
CROSS JOIN customer_count cc
JOIN lakehouse.cdc.silver_customers c
    -- Cast to INT assuming your Debezium c.id is an INT
    ON CAST(MOD(t.trip_id, cc.cnt) + 1 AS INT) = c.id 
LIMIT 20;