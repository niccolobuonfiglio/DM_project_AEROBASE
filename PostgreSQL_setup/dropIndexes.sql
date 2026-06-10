-- =============================================================================
-- DROP SPECIFIC INDEXES (by name)
-- =============================================================================

DROP INDEX IF EXISTS idx_flights_carrier;
DROP INDEX IF EXISTS idx_flights_origin;
DROP INDEX IF EXISTS idx_flights_dest;
DROP INDEX IF EXISTS idx_flights_tail_num;
DROP INDEX IF EXISTS idx_delays_flight_id;
DROP INDEX IF EXISTS idx_flights_year_month;
DROP INDEX IF EXISTS idx_flights_year;
DROP INDEX IF EXISTS idx_delays_arr_delay;
DROP INDEX IF EXISTS idx_delays_dep_delay;
DROP INDEX IF EXISTS idx_flights_cancelled;
DROP INDEX IF EXISTS idx_aircraft_primary_airline;
DROP INDEX IF EXISTS idx_airports_city;
DROP INDEX IF EXISTS idx_airports_country;
DROP INDEX IF EXISTS idx_cities_country;
DROP INDEX IF EXISTS idx_airports_location;

RAISE NOTICE 'All specified indexes have been dropped!';
