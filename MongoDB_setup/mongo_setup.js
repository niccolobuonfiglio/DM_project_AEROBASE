// MongoDB Setup for Flight Data Analysis Project
// Run in mongosh against the target database:
// mongosh "mongodb://localhost:27017/flightdb" --file mongo_setup.js

use("flightdb");

// Collections

// Safety guard: abort if any collection already has documents.
const flightCount = db.flights.countDocuments({});
if (flightCount > 0) {
    print("ERROR: flights collection already contains " + flightCount +
          " documents. Aborting to avoid data loss.");
    print("If you really want to reset, run this in mongosh first:");
    print("  db.flights.drop(); db.airports.drop(); db.airlines.drop();");
    quit(1);
}

db.flights.drop();
db.airports.drop();
db.airlines.drop();

// flights: denormalized fact collection (embedded carrier, origin, dest).
db.createCollection("flights", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["flight_id", "year", "month", "carrier", "origin", "dest",
                       "cancelled", "diverted"],
            properties: {
                flight_id:    { bsonType: "long" },
                year:         { bsonType: "int" },
                month:        { bsonType: "int", minimum: 1, maximum: 12 },
                day_of_month: { bsonType: "int", minimum: 1, maximum: 31 },
                day_of_week:  { bsonType: "int", minimum: 1, maximum: 7 },
                carrier:      { bsonType: "object" },
                origin:       { bsonType: "object" },
                dest:         { bsonType: "object" },
                cancelled:    { bsonType: "bool" },
                diverted:     { bsonType: "bool" }
            }
        }
    },
    validationLevel:  "moderate",   
    validationAction: "warn"        
});

db.createCollection("airports");
db.createCollection("airlines");

print("Collections created.");



