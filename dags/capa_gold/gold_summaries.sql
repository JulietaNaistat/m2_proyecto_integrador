
-- DROP existing gold tables if they exist
DROP TABLE IF EXISTS gold.listing_summary CASCADE;
DROP TABLE IF EXISTS gold.host_summary CASCADE;
DROP TABLE IF EXISTS gold.neighbourhood_summary CASCADE;

-- Create gold table: listing_summary
CREATE TABLE gold.listing_summary (
    listing_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    host_name VARCHAR(255),
    neighbourhood VARCHAR(255),
    neighbourhood_group VARCHAR(255),
    room_type VARCHAR(255),
    price INTEGER,
    minimum_nights INTEGER,
    availability_365 INTEGER,
    number_of_reviews INTEGER,
    reviews_per_month NUMERIC,
    last_review DATE
);

-- Create gold table: host_summary
CREATE TABLE gold.host_summary (
    host_id BIGINT PRIMARY KEY,
    host_name VARCHAR(255),
    total_listings INTEGER,
    total_reviews INTEGER,
    avg_reviews_per_month NUMERIC
);

-- Create gold table: neighbourhood_summary
CREATE TABLE gold.neighbourhood_summary (
    neighbourhood_id TEXT PRIMARY KEY,
    neighbourhood VARCHAR(255),
    neighbourhood_group VARCHAR(255),
    total_listings INTEGER,
    avg_price NUMERIC,
    avg_availability INTEGER
);

-- Insert into gold.listing_summary
INSERT INTO gold.listing_summary
SELECT
    l.id AS listing_id,
    l.name,
    h.host_name,
    n.neighbourhood,
    n.neighbourhood_group,
    l.room_type,
    l.price,
    l.minimum_nights,
    l.availability_365,
    r.number_of_reviews,
    r.reviews_per_month,
    r.last_review
FROM silver.listings l
JOIN silver.hosts h ON l.host_id = h.id
JOIN silver.neighbourhoods n ON l.neighbourhood_id = n.id
LEFT JOIN silver.reviews r ON l.id = r.listing_id;

-- Insert into gold.host_summary
INSERT INTO gold.host_summary
SELECT
    h.id AS host_id,
    h.host_name,
    COUNT(l.id) AS total_listings,
    COALESCE(SUM(r.number_of_reviews), 0) AS total_reviews,
    COALESCE(AVG(r.reviews_per_month), 0) AS avg_reviews_per_month
FROM silver.hosts h
LEFT JOIN silver.listings l ON h.id = l.host_id
LEFT JOIN silver.reviews r ON l.id = r.listing_id
GROUP BY h.id, h.host_name;

-- Insert into gold.neighbourhood_summary
INSERT INTO gold.neighbourhood_summary
SELECT
    n.id AS neighbourhood_id,
    n.neighbourhood,
    n.neighbourhood_group,
    COUNT(l.id) AS total_listings,
    AVG(l.price) AS avg_price,
    AVG(l.availability_365) AS avg_availability
FROM silver.neighbourhoods n
LEFT JOIN silver.listings l ON n.id = l.neighbourhood_id
GROUP BY n.id, n.neighbourhood, n.neighbourhood_group;
