-- PostgreSQL Schema for AEROBASE Project 
-- Load order: cities -> airports -> airlines -> routes -> calendar -> aircraft -> flights
-- Usage: psql -U admin -d flightdb -h localhost -f schema.sql

-- Drop in reverse dependency order 
DROP TABLE IF EXISTS flights CASCADE;
DROP TABLE IF EXISTS routes CASCADE;
DROP TABLE IF EXISTS calendar CASCADE;
DROP TABLE IF EXISTS aircraft CASCADE;
DROP TABLE IF EXISTS airlines CASCADE;
DROP TABLE IF EXISTS airports CASCADE;
DROP TABLE IF EXISTS cities CASCADE;


-- cities 
CREATE TABLE cities (
    city_name        VARCHAR(100) PRIMARY KEY,
    country          VARCHAR(50)  NOT NULL DEFAULT 'United States',
    timezone_offset  REAL,                    -- Hours offset from UTC
    dst              CHAR(1),
    tz_database      TEXT
);

COMMENT ON TABLE  cities                    IS 'City information for airport locations';
COMMENT ON COLUMN cities.city_name          IS 'Name of the city (primary key)';
COMMENT ON COLUMN cities.country            IS 'Country name';
COMMENT ON COLUMN cities.timezone_offset    IS 'Hours offset from UTC (can be fractional, e.g. -3.5)';


-- airports (dimension table)
CREATE TABLE airports (
    iata_code        CHAR(3)          PRIMARY KEY,
    icao_code        CHAR(4),
    name             TEXT             NOT NULL,
    city_name        VARCHAR(100)     REFERENCES cities(city_name),
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    altitude         INTEGER,
    type             VARCHAR(20),      -- 'airport', 'station', etc.
    source           VARCHAR(50)       -- Data source
);

COMMENT ON TABLE  airports                    IS 'US airports (incl. territories) from OpenFlights';
COMMENT ON COLUMN airports.iata_code          IS '3-letter IATA code, primary key';
COMMENT ON COLUMN airports.city_name          IS 'Foreign key to cities table';


-- airlines 
CREATE TABLE airlines (
    unique_carrier           VARCHAR(3)  PRIMARY KEY,
    airline_name             TEXT,
    country_of_registration  TEXT,
    icao_code                VARCHAR(4),
    is_defunct               BOOLEAN,
    source                   TEXT
);

COMMENT ON TABLE  airlines                         IS 'Carriers seen in flights data';
COMMENT ON COLUMN airlines.unique_carrier          IS 'DOT UniqueCarrier code (usually IATA)';
COMMENT ON COLUMN airlines.is_defunct              IS 'TRUE = known defunct, FALSE = known active, NULL = unknown';


-- routes (unique flight paths)
CREATE TABLE routes (
    route_id         BIGSERIAL    PRIMARY KEY,
    origin           CHAR(3)      NOT NULL REFERENCES airports(iata_code),
    dest             CHAR(3)      NOT NULL REFERENCES airports(iata_code),
    distance         INTEGER,
    
    -- Ensure uniqueness per route direction
    UNIQUE(origin, dest)
);

COMMENT ON TABLE  routes                       IS 'Unique flight routes (origin-destination pairs)';
COMMENT ON COLUMN routes.route_id              IS 'Auto-generated unique identifier for each route';
COMMENT ON COLUMN routes.origin                IS 'Origin airport IATA code';
COMMENT ON COLUMN routes.dest                  IS 'Destination airport IATA code';
COMMENT ON COLUMN routes.distance              IS 'Distance between airports in miles';


-- calendar 
CREATE TABLE calendar (
    calendar_id      BIGSERIAL    PRIMARY KEY,
    year             SMALLINT     NOT NULL,
    month            SMALLINT     NOT NULL,
    day_of_month     SMALLINT     NOT NULL,
    day_of_week      SMALLINT     NOT NULL,
    UNIQUE(year, month, day_of_month, day_of_week)
);

COMMENT ON TABLE  calendar                       IS 'Date dimension for flight scheduling';
COMMENT ON COLUMN calendar.calendar_id           IS 'Auto-generated unique identifier for each date';
COMMENT ON COLUMN calendar.year                  IS 'Year (e.g., 2007, 2008)';
COMMENT ON COLUMN calendar.month                 IS 'Month (1-12)';
COMMENT ON COLUMN calendar.day_of_month          IS 'Day of month (1-31)';
COMMENT ON COLUMN calendar.day_of_week           IS 'Day of week (1-7, configurable)';
COMMENT ON COLUMN calendar.quarter               IS 'Quarter (1-4)';
COMMENT ON COLUMN calendar.is_weekend            IS 'True if Saturday or Sunday';


-- aircraft 
CREATE TABLE aircraft (
    tail_num            VARCHAR(10)  PRIMARY KEY,
    first_flight_year   SMALLINT,               -- First year this aircraft appeared
    last_flight_year    SMALLINT,               -- Last year this aircraft appeared
    primary_airline     VARCHAR(3)  REFERENCES airlines(unique_carrier),  -- Airline that operated it most
    total_flights       INTEGER                 -- Total number of flights
);

COMMENT ON TABLE  aircraft IS 'Aircraft summary derived from flight history';
COMMENT ON COLUMN aircraft.tail_num IS 'FAA tail number (N + 1-5 alphanumeric chars)';
COMMENT ON COLUMN aircraft.first_flight_year IS 'First year this aircraft appears in flights';
COMMENT ON COLUMN aircraft.last_flight_year IS 'Last year this aircraft appears in flights';
COMMENT ON COLUMN aircraft.primary_airline IS 'Airline that operated this aircraft most frequently';
COMMENT ON COLUMN aircraft.total_flights IS 'Total number of flights recorded for this aircraft';


-- flights 
CREATE TABLE flights (
    flight_id              BIGINT       PRIMARY KEY,
    tail_num               TEXT         REFERENCES aircraft(tail_num),
    unique_carrier         VARCHAR(3)   NOT NULL REFERENCES airlines(unique_carrier),
    route_id               BIGINT       NOT NULL REFERENCES routes(route_id),
    calendar_id            BIGINT       NOT NULL REFERENCES calendar(calendar_id),
    flight_num             INTEGER,
    cancelled              BOOLEAN      NOT NULL,
    diverted               BOOLEAN      NOT NULL,
    cancellation_code      CHAR(1),
    
    -- Time fields (HHMM format)
    crs_dep_time           INTEGER,
    dep_time               INTEGER,
    crs_arr_time           INTEGER,
    arr_time               INTEGER,
    
    -- Duration fields
    crs_elapsed_time       INTEGER,
    actual_elapsed_time    INTEGER,
    air_time               INTEGER,
    taxi_in                INTEGER,
    taxi_out               INTEGER,
    
    -- Delay fields 
    arr_delay              INTEGER,              
    dep_delay              INTEGER,               
    carrier_delay          REAL,                 
    weather_delay          REAL,                  
    nas_delay              REAL,                  
    security_delay         REAL,               
    late_aircraft_delay    REAL                  
);

COMMENT ON TABLE  flights                         IS 'Core flight information including delay data';
COMMENT ON COLUMN flights.flight_id               IS 'Sequential ID assigned during preprocessing';
COMMENT ON COLUMN flights.tail_num                IS 'Foreign key to aircraft table';
COMMENT ON COLUMN flights.unique_carrier          IS 'Foreign key to airlines table';
COMMENT ON COLUMN flights.route_id                IS 'Foreign key to routes table (origin-dest pair)';
COMMENT ON COLUMN flights.calendar_id             IS 'Foreign key to calendar table (year-month-day)';
COMMENT ON COLUMN flights.cancellation_code       IS 'A=carrier, B=weather, C=NAS, D=security; NULL if not cancelled';
COMMENT ON COLUMN flights.carrier_delay           IS 'Delay cause in minutes; NULL before June 2003';
