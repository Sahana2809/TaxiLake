-- Star schema for the trip warehouse.
-- Runs automatically on first container boot (docker-entrypoint-initdb.d);
-- see warehouse/schema.sql for the standalone copy used on manual (re)apply.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY,        -- YYYYMMDD
    full_date    DATE NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,           -- 0=Sunday .. 6=Saturday
    day_name     VARCHAR(9) NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_zone (
    zone_key      INTEGER PRIMARY KEY,        -- TLC LocationID
    borough       VARCHAR(20) NOT NULL,
    zone          VARCHAR(100) NOT NULL,
    service_zone  VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_key  SMALLINT PRIMARY KEY,
    description        VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pickup_date_key     INTEGER NOT NULL REFERENCES dim_date(date_key),
    pickup_zone_key     INTEGER NOT NULL REFERENCES dim_zone(zone_key),
    dropoff_zone_key    INTEGER NOT NULL REFERENCES dim_zone(zone_key),
    payment_type_key    SMALLINT REFERENCES dim_payment_type(payment_type_key),
    vendor_id           SMALLINT,
    pickup_datetime     TIMESTAMP NOT NULL,
    dropoff_datetime    TIMESTAMP NOT NULL,
    passenger_count     SMALLINT,
    trip_distance       DOUBLE PRECISION,
    trip_duration_min   DOUBLE PRECISION,
    fare_amount         NUMERIC(10, 2),
    tip_amount          NUMERIC(10, 2),
    tolls_amount        NUMERIC(10, 2),
    total_amount        NUMERIC(10, 2)
);

CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_date ON fact_trips(pickup_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_zone ON fact_trips(pickup_zone_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_dropoff_zone ON fact_trips(dropoff_zone_key);

INSERT INTO dim_payment_type (payment_type_key, description) VALUES
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip')
ON CONFLICT (payment_type_key) DO NOTHING;
