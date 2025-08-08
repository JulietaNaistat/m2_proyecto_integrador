--Se crea una vista desnormalizando todas las tablas en una sola, dedicada para ML o creacion de Dashboards.
CREATE OR REPLACE VIEW gold.listings_OBT AS
SELECT
    l.id AS listing_id,
    l.name AS listing_name,
    l.latitude,
    l.longitude,
    l.room_type,
    l.price,
    l.minimum_nights,
    l.availability_365,
    h.id AS host_id,
    h.host_name,
    h.calculated_host_listings_count,
    n.id AS neighbourhood_id,
    n.neighbourhood,
    n.neighbourhood_group,
    r.id AS review_id,
    r.number_of_reviews,
    r.last_review,
    r.reviews_per_month,
    r.review_title,
    r.review_descrciption,
    r.review_date,
    l.price * ars.value, 2 AS price_ARS,
    l.price * eur.value AS price_EUR,
    l.price * gbp.value AS price_GBP
FROM silver.listings l
LEFT JOIN silver.hosts h ON l.host_id = h.id
LEFT JOIN silver.neighbourhoods n ON l.neighbourhood_id = n.id
LEFT JOIN silver.reviews r ON l.id = r.listing_id
LEFT JOIN silver.currencies ars ON ars.currency = 'ARS'
LEFT JOIN silver.currencies eur ON eur.currency = 'EUR'
LEFT JOIN silver.currencies gbp ON gbp.currency = 'GBP';