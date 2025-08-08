
-- Crear tablas normalizadas en el schema silver

-- Drop y Create neighbourhoods
DROP TABLE IF EXISTS silver.neighbourhoods CASCADE;
CREATE TABLE silver.neighbourhoods (
    id TEXT PRIMARY KEY,
    neighbourhood VARCHAR(255),
    neighbourhood_group VARCHAR(255)
);
-- Insertar datos en silver.neighbourhoods
INSERT INTO silver.neighbourhoods (
    id, neighbourhood, neighbourhood_group
)
SELECT DISTINCT
    hashtext(neighbourhood) as id,
    neighbourhood,
    neighbourhood_group
FROM bronze.ab_nyc;

-- Drop y Create hosts
DROP TABLE IF EXISTS silver.hosts CASCADE;
CREATE TABLE silver.hosts (
    id BIGINT PRIMARY KEY,
    host_name VARCHAR(255),
    calculated_host_listings_count INTEGER
);
-- Insertar datos en silver.hosts
INSERT INTO silver.hosts (
    id, host_name, calculated_host_listings_count
)
SELECT DISTINCT
    host_id as id, 
    host_name, 
    calculated_host_listings_count
FROM bronze.ab_nyc;

-- Drop y Create listings
DROP TABLE IF EXISTS silver.listings CASCADE;
CREATE TABLE silver.listings (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    host_id BIGINT REFERENCES silver.hosts(id),
    neighbourhood_id TEXT REFERENCES silver.neighbourhoods(id),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    room_type VARCHAR(255),
    price INTEGER,
    minimum_nights INTEGER,
    availability_365 INTEGER
);
-- Insertar datos normalizados en silver.listings
INSERT INTO silver.listings (
    id, name, host_id, neighbourhood_id, latitude, longitude, room_type, price, minimum_nights, availability_365
)
SELECT DISTINCT
    id, 
    name,
    host_id, 
    hashtext(neighbourhood) as neighbourhood_id,
    latitude, 
    longitude, 
    room_type, 
    price, 
    minimum_nights, 
    availability_365
FROM bronze.ab_nyc;

-- Drop y Create reviews
DROP TABLE IF EXISTS silver.reviews CASCADE;
CREATE TABLE silver.reviews (
    id TEXT PRIMARY KEY,
    listing_id BIGINT REFERENCES silver.listings(id),
    number_of_reviews INTEGER,
    last_review DATE,
    reviews_per_month NUMERIC,
    review_title VARCHAR(255),
    review_descrciption VARCHAR(255),
    review_date DATE
);
-- Insertar datos en silver.reviews
INSERT INTO silver.reviews (
    id, listing_id, number_of_reviews, last_review, reviews_per_month, review_title, review_descrciption, review_date
)
SELECT DISTINCT
    hashtext(CONCAT(COALESCE(last_review, 'No reviews'), id)) as id,
    id as listing_id,
    number_of_reviews, 
    CAST(last_review AS DATE) AS last_review, 
    COALESCE(reviews_per_month, 0) as reviews_per_month,
    NULL as review_title,
    NULL as review_descrciption,
    CAST(NULL AS DATE) as review_date
FROM bronze.ab_nyc;


