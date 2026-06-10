-- INDEX CREATION: drops if exists, then creates

-- Routes table indexes
DROP INDEX IF EXISTS idx_routes_origin;
CREATE INDEX idx_routes_origin ON routes (origin);

DROP INDEX IF EXISTS idx_routes_dest;
CREATE INDEX idx_routes_dest ON routes (dest);

-- Calendar table indexes
DROP INDEX IF EXISTS idx_calendar_year;
CREATE INDEX idx_calendar_year ON calendar (year);

DROP INDEX IF EXISTS idx_calendar_year_month;
CREATE INDEX idx_calendar_year_month ON calendar (year, month);

-- Foreign key indexes on flights
DROP INDEX IF EXISTS idx_flights_carrier;
CREATE INDEX idx_flights_carrier ON flights (unique_carrier);

DROP INDEX IF EXISTS idx_flights_route;
CREATE INDEX idx_flights_route ON flights (route_id);

DROP INDEX IF EXISTS idx_flights_calendar;
CREATE INDEX idx_flights_calendar ON flights (calendar_id);

DROP INDEX IF EXISTS idx_flights_tail_num;
CREATE INDEX idx_flights_tail_num ON flights (tail_num) WHERE tail_num IS NOT NULL;

-- Delay analysis indexes
DROP INDEX IF EXISTS idx_flights_arr_delay;
CREATE INDEX idx_flights_arr_delay ON flights (arr_delay) WHERE arr_delay IS NOT NULL;

DROP INDEX IF EXISTS idx_flights_dep_delay;
CREATE INDEX idx_flights_dep_delay ON flights (dep_delay) WHERE dep_delay IS NOT NULL;

-- Cancellation analysis
DROP INDEX IF EXISTS idx_flights_cancelled;
CREATE INDEX idx_flights_cancelled ON flights (cancelled, calendar_id) WHERE cancelled = true;

-- Aircraft queries
DROP INDEX IF EXISTS idx_aircraft_primary_airline;
CREATE INDEX idx_aircraft_primary_airline ON aircraft (primary_airline);

-- Airport/city lookups
DROP INDEX IF EXISTS idx_airports_city;
CREATE INDEX idx_airports_city ON airports (city_name);

DROP INDEX IF EXISTS idx_cities_country;
CREATE INDEX idx_cities_country ON cities (country);

-- Run ANALYZE to update statistics
ANALYZE;

-- Verify indexes exist
SELECT 
    schemaname,
    tablename,
    indexname
FROM pg_indexes 
WHERE indexname LIKE 'idx_%'
ORDER BY tablename, indexname;
