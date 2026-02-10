-- init-db.sql
-- Automatically creates and populates the crime_data table on first PostgreSQL start.
-- To switch datasets, change the COPY path from 'tiny' to 'large'.

CREATE TABLE IF NOT EXISTS crime_data (
    id            INTEGER PRIMARY KEY,
    district      VARCHAR(100),
    datetime      TIMESTAMP,
    primary_type  VARCHAR(50),
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION
);

COPY crime_data (id, district, datetime, primary_type, lat, lon)
FROM '/project_data/datasets/large/crime_data.csv'
DELIMITER ','
CSV HEADER;
