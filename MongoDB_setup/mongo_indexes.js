// Drop all indexes first, then create

print("Dropping all existing indexes...");

// Drop all user-defined indexes (keep _id)
db.flights.dropIndexes();
db.airports.dropIndexes();
db.airlines.dropIndexes();
db.aircraft.dropIndexes();

print("All indexes dropped. Creating fresh indexes...\n");

// Create all indexes
db.flights.createIndex({ flight_id: 1 }, { unique: true, name: "idx_flight_id" });
db.flights.createIndex({ tail_num: 1 }, { name: "idx_tail_num" });
db.flights.createIndex({ "carrier.code": 1 }, { name: "idx_carrier_code" });
db.flights.createIndex({ "origin.iata": 1 }, { name: "idx_origin_iata" });
db.flights.createIndex({ "dest.iata": 1 }, { name: "idx_dest_iata" });
db.flights.createIndex({ year: 1, month: 1 }, { name: "idx_year_month" });
db.flights.createIndex({ year: 1 }, { name: "idx_year" });
db.flights.createIndex({ year: 1, "carrier.code": 1 });
db.flights.createIndex({ "delays.arr_delay": 1 }, { name: "idx_arr_delay" });
db.flights.createIndex({ "delays.dep_delay": 1 }, { name: "idx_dep_delay" });
db.flights.createIndex({ year: 1, cancelled: 1, "origin.iata": 1 }, { name: "idx_cancelled" });
db.flights.createIndex({ tail_num: 1, year: 1 }, { name: "idx_tail_num_year" });
db.flights.createIndex({ "origin.location": "2dsphere" }, { name: "idx_origin_location" });
db.flights.createIndex({ "dest.location": "2dsphere" }, { name: "idx_dest_location" });
db.flights.createIndex({ year: 1, "carrier.code": 1, "delays.arr_delay": 1 }, { name: "idx_year_carrier_delay" });
db.flights.createIndex({ "origin.iata": 1, "dest.iata": 1, year: 1 }, { name: "idx_origin_dest_year" });
db.flights.createIndex({ tail_num: 1, year: 1, month: 1, day_of_month: 1 }, { name: "idx_tail_num_date" });

// Dimension tables
db.airports.createIndex({ iata_code: 1 }, { unique: true, name: "idx_iata_code" });
db.airports.createIndex({ city_name: 1 }, { name: "idx_airports_city" });
db.airports.createIndex({ country: 1 }, { name: "idx_airports_country" });
db.airports.createIndex({ location: "2dsphere" }, { name: "idx_airports_location" });

db.airlines.createIndex({ unique_carrier: 1 }, { unique: true, name: "idx_unique_carrier" });

db.aircraft.createIndex({ tail_num: 1 }, { unique: true, name: "idx_aircraft_tail_num" });
db.aircraft.createIndex({ primary_airline: 1 }, { name: "idx_primary_airline" });

print("All indexes created successfully!");