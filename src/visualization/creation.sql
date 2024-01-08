-- Active: 1680532809848@@192.168.1.23@15432
-- Création des tables de dimensions
CREATE TABLE date_dim (
    date_id SERIAL PRIMARY KEY,
    date DATE,
    day_of_week INT,
    month INT,
    year INT
);

CREATE TABLE time_dim (
    time_id SERIAL PRIMARY KEY,
    hour INT,
    minute INT,
    second INT
);

-- Création de la table de faits
CREATE TABLE taxi_trips (
    trip_id SERIAL PRIMARY KEY,
    pickup_date_id INT,
    pickup_time_id INT,
    dropoff_date_id INT,
    dropoff_time_id INT,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    total_amount FLOAT,
    FOREIGN KEY (pickup_date_id) REFERENCES date_dim(date_id),
    FOREIGN KEY (pickup_time_id) REFERENCES time_dim(time_id),
    FOREIGN KEY (dropoff_date_id) REFERENCES date_dim(date_id),
    FOREIGN KEY (dropoff_time_id) REFERENCES time_dim(time_id)
);
