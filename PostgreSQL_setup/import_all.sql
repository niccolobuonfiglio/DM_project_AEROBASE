-- Import script for AEROBASE

-- Staging tables that match CSV structure exactly

-- Staging for airports
DROP TABLE IF EXISTS airports_staging CASCADE;
CREATE TABLE airports_staging (
    iata_code CHAR(3),
    icao_code CHAR(4),
    name TEXT,
    city TEXT,
    country TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude INTEGER,
    timezone_offset REAL,
    dst CHAR(1),
    tz_database TEXT
);

-- Staging for airlines
DROP TABLE IF EXISTS airlines_staging CASCADE;
CREATE TABLE airlines_staging (
    unique_carrier VARCHAR(3),
    airline_name TEXT,
    country_of_registration TEXT,
    icao_code VARCHAR(4),
    is_defunct VARCHAR(10),
    source TEXT
);

-- Staging for flights
DROP TABLE IF EXISTS flights_staging CASCADE;
CREATE TABLE flights_staging (
    flight_id BIGINT,
    actual_elapsed_time INTEGER,
    air_time INTEGER,
    arr_delay INTEGER,
    arr_time INTEGER,
    crs_arr_time INTEGER,
    crs_dep_time INTEGER,
    crs_elapsed_time INTEGER,
    cancellation_code CHAR(1),
    cancelled VARCHAR(1),
    carrier_delay REAL,
    day_of_week SMALLINT,
    day_of_month SMALLINT,
    dep_delay INTEGER,
    dep_time INTEGER,
    dest CHAR(3),
    distance INTEGER,
    diverted VARCHAR(1),
    flight_num INTEGER,
    late_aircraft_delay REAL,
    month SMALLINT,
    nas_delay REAL,
    origin CHAR(3),
    security_delay REAL,
    tail_num TEXT,
    taxi_in INTEGER,
    taxi_out INTEGER,
    unique_carrier VARCHAR(3),
    weather_delay REAL,
    year SMALLINT
);

-- Import CSV files into staging tables

\echo 'Importing airports from CSV...'
\COPY airports_staging FROM 'data/output/airports.csv' WITH (FORMAT CSV, HEADER, NULL '')

\echo 'Importing airlines from CSV...'
\COPY airlines_staging FROM 'data/output/airlines.csv' WITH (FORMAT CSV, HEADER, NULL '')

\echo 'Importing flights from CSV...'
\COPY flights_staging FROM 'data/output/flights.csv' WITH (FORMAT CSV, HEADER, NULL '')

-- Populate cities table
\echo 'Populating cities table...'
TRUNCATE cities CASCADE;

INSERT INTO cities (city_name, country, timezone_offset, dst, tz_database)
SELECT DISTINCT ON (city)
    city,
    country,
    timezone_offset,
    dst,
    tz_database
FROM airports_staging
WHERE city IS NOT NULL 
  AND city != ''
  AND country = 'United States'
ORDER BY city, tz_database NULLS LAST;

SELECT COUNT(*) as cities_count FROM cities;

-- Populate airports table

\echo 'Populating airports table...'
TRUNCATE airports CASCADE;

INSERT INTO airports (iata_code, icao_code, name, city_name, latitude, longitude, altitude, type, source)
SELECT 
    s.iata_code,
    s.icao_code,
    s.name,
    s.city as city_name,
    s.latitude,
    s.longitude,
    s.altitude,
    'airport' as type,
    'openflights' as source
FROM airports_staging s
WHERE s.iata_code IS NOT NULL
  AND s.country = 'United States'
  AND EXISTS (SELECT 1 FROM cities c WHERE c.city_name = s.city)
ON CONFLICT (iata_code) DO NOTHING;

SELECT COUNT(*) as airports_count FROM airports;

-- Populate airlines table

\echo 'Populating airlines table...'
TRUNCATE airlines CASCADE;

INSERT INTO airlines (unique_carrier, airline_name, country_of_registration, icao_code, is_defunct, source)
SELECT 
    unique_carrier,
    airline_name,
    country_of_registration,
    icao_code,
    CASE 
        WHEN is_defunct = 't' THEN TRUE
        WHEN is_defunct = 'f' THEN FALSE
        ELSE NULL
    END as is_defunct,
    source
FROM airlines_staging
WHERE unique_carrier IS NOT NULL
  AND unique_carrier != '';

SELECT COUNT(*) as airlines_count FROM airlines;

-- Populate routes table (unique origin-dest pairs with distance)
\echo 'Populating routes table...'
TRUNCATE routes CASCADE;

INSERT INTO routes (origin, dest, distance)
SELECT DISTINCT
    origin,
    dest,
    MIN(distance) as distance  
FROM flights_staging
WHERE origin IS NOT NULL 
  AND dest IS NOT NULL
  AND EXISTS (SELECT 1 FROM airports a WHERE a.iata_code = origin)
  AND EXISTS (SELECT 1 FROM airports a WHERE a.iata_code = dest)
GROUP BY origin, dest
ON CONFLICT (origin, dest) DO NOTHING;

SELECT COUNT(*) as routes_count FROM routes;

-- Populate calendar table (unique dates from flights)

\echo 'Populating calendar table...'
TRUNCATE calendar CASCADE;

-- Insert unique dates from flights_staging
INSERT INTO calendar (year, month, day_of_month, day_of_week)
SELECT DISTINCT
    year,
    month,
    day_of_month,
    day_of_week
FROM flights_staging
WHERE year IS NOT NULL 
  AND month IS NOT NULL
  AND day_of_month IS NOT NULL
  AND day_of_week IS NOT NULL
ORDER BY year, month, day_of_month;

SELECT COUNT(*) as calendar_count FROM calendar;

-- Populate aircraft table 
\echo 'Populating aircraft table...'
TRUNCATE aircraft CASCADE;

INSERT INTO aircraft (tail_num, first_flight_year, last_flight_year, total_flights, primary_airline)
SELECT 
    tail_num,
    MIN(year) as first_flight_year,
    MAX(year) as last_flight_year,
    COUNT(*) as total_flights,
    MODE() WITHIN GROUP (ORDER BY unique_carrier) as primary_airline
FROM flights_staging
WHERE tail_num IS NOT NULL
  AND tail_num != ''
  AND tail_num != 'NAN'
GROUP BY tail_num;

SELECT COUNT(*) as aircraft_count FROM aircraft;

-- Populate flights table (with route_id and calendar_id foreign keys)
\echo 'Populating flights table...'
TRUNCATE flights CASCADE;

INSERT INTO flights (
    flight_id, tail_num, unique_carrier, route_id, calendar_id, flight_num,
    cancelled, diverted, cancellation_code,
    crs_dep_time, dep_time, crs_arr_time, arr_time,
    crs_elapsed_time, actual_elapsed_time, air_time, taxi_in, taxi_out,
    arr_delay, dep_delay, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay
)
SELECT 
    s.flight_id,
    NULLIF(s.tail_num, '') as tail_num,
    s.unique_carrier,
    r.route_id,
    c.calendar_id,
    s.flight_num,
    s.cancelled = 't',
    s.diverted = 't',
    NULLIF(s.cancellation_code, ''),
    s.crs_dep_time,
    s.dep_time,
    s.crs_arr_time,
    s.arr_time,
    s.crs_elapsed_time,
    s.actual_elapsed_time,
    s.air_time,
    s.taxi_in,
    s.taxi_out,
    s.arr_delay,
    s.dep_delay,
    s.carrier_delay,
    s.weather_delay,
    s.nas_delay,
    s.security_delay,
    s.late_aircraft_delay
FROM flights_staging s
JOIN routes r ON r.origin = s.origin AND r.dest = s.dest
JOIN calendar c ON c.year = s.year 
                AND c.month = s.month 
                AND c.day_of_month = s.day_of_month
WHERE s.flight_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM airlines a WHERE a.unique_carrier = s.unique_carrier)
  AND EXISTS (SELECT 1 FROM routes r2 WHERE r2.origin = s.origin AND r2.dest = s.dest)
  AND EXISTS (SELECT 1 FROM calendar c2 WHERE c2.year = s.year 
                AND c2.month = s.month 
                AND c2.day_of_month = s.day_of_month);

SELECT COUNT(*) as flights_count FROM flights;

-- Set NULL for tail_num that don't exist in aircraft table
\echo 'Fixing aircraft references in flights...'
UPDATE flights 
SET tail_num = NULL 
WHERE tail_num IS NOT NULL 
  AND NOT EXISTS (SELECT 1 FROM aircraft a WHERE a.tail_num = flights.tail_num);

SELECT COUNT(*) as flights_with_valid_aircraft 
FROM flights 
WHERE tail_num IS NOT NULL;

-- Add aircraft foreign key constraint
\echo 'Adding aircraft foreign key constraint...'
ALTER TABLE flights 
DROP CONSTRAINT IF EXISTS fk_flights_aircraft;

ALTER TABLE flights 
ADD CONSTRAINT fk_flights_aircraft 
FOREIGN KEY (tail_num) REFERENCES aircraft(tail_num);

-- Clean up: drop staging tables
\echo 'Cleaning up staging tables...'
DROP TABLE IF EXISTS airports_staging;
DROP TABLE IF EXISTS airlines_staging;
DROP TABLE IF EXISTS flights_staging;

-- Final statistics

\echo ''
\echo '============================================================'
\echo 'IMPORT COMPLETE - FINAL STATISTICS'
\echo '============================================================'

SELECT 'cities' as table_name, COUNT(*) as rows FROM cities
UNION ALL
SELECT 'airports', COUNT(*) FROM airports
UNION ALL
SELECT 'airlines', COUNT(*) FROM airlines
UNION ALL
SELECT 'routes', COUNT(*) FROM routes
UNION ALL
SELECT 'calendar', COUNT(*) FROM calendar
UNION ALL
SELECT 'aircraft', COUNT(*) FROM aircraft
UNION ALL
SELECT 'flights', COUNT(*) FROM flights
ORDER BY rows DESC;

\echo ''
\echo 'Sample data verification:'
\echo '------------------------'
SELECT 'Routes sample:' as info;
SELECT route_id, origin, dest, distance FROM routes LIMIT 10;

SELECT 'Calendar sample:' as info;
SELECT calendar_id, year, month, day_of_month, day_of_week FROM calendar LIMIT 10;

SELECT 'Flights sample:' as info;
SELECT f.flight_id, f.tail_num, f.unique_carrier, r.origin, r.dest, 
       c.year, c.month, c.day_of_month, f.arr_delay
FROM flights f
JOIN routes r ON f.route_id = r.route_id
JOIN calendar c ON f.calendar_id = c.calendar_id
LIMIT 10;

SELECT 'Flights with aircraft count:' as info;
SELECT COUNT(*) as flights_with_aircraft FROM flights WHERE tail_num IS NOT NULL;

\echo ''
\echo '============================================================'
\echo 'IMPORT COMPLETE SUCCESSFULLY!'
\echo '============================================================'
