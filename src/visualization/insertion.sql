-- Active: 1680532809848@@192.168.1.23@15432
-- Insertion dans la table de dimensions 'date_dim'
INSERT INTO date_dim (date, day_of_week, month, year)
SELECT DISTINCT
    DATE(tpep_pickup_datetime) AS date,
    EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS year
FROM nyc_raw;

-- Insertion dans la table de dimensions 'time_dim'
INSERT INTO time_dim (hour, minute)
SELECT DISTINCT
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
    EXTRACT(MINUTE FROM tpep_pickup_datetime) AS minute
FROM nyc_raw;

-- Insertion dans la table de faits 'taxi_trips'
INSERT INTO taxi_trips (
    pickup_date_id, pickup_time_id, dropoff_date_id, dropoff_time_id, passenger_count, trip_distance, fare_amount, total_amount
)
SELECT
    (SELECT date_id FROM date_dim WHERE date = DATE(tpep_pickup_datetime)),
    (SELECT time_id FROM time_dim WHERE hour = EXTRACT(HOUR FROM tpep_pickup_datetime) AND minute = EXTRACT(MINUTE FROM tpep_pickup_datetime)),
    (SELECT date_id FROM date_dim WHERE date = DATE(tpep_dropoff_datetime)),
    (SELECT time_id FROM time_dim WHERE hour = EXTRACT(HOUR FROM tpep_dropoff_datetime) AND minute = EXTRACT(MINUTE FROM tpep_dropoff_datetime)),
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount
FROM nyc_raw;
