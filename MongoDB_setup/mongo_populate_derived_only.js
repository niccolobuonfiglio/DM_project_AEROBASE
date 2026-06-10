// MongoDB Populate Derived Collections 
// AFTER flights data is already imported:
// mongosh "mongodb://localhost:27017/flightdb" --file mongo_populate_derived_only.js
// This script creates and populates derived collections (aircraft, cities)

use("flightdb");

print("=".repeat(60));
print("POPULATING DERIVED COLLECTIONS (matching relational schema)");
print("=".repeat(60));

// Check if flights data exists
const flightCount = db.flights.countDocuments();
if (flightCount === 0) {
    print("❌ ERROR: No flights data found.");
    quit(1);
}
print(`✅ Found ${flightCount.toLocaleString()} flights documents`);

// AIRCRAFT Collection 
print("\n📝 1. Building aircraft collection (matching relational schema)...");
db.aircraft.drop();

// First pass: get basic aircraft stats
db.flights.aggregate([
    { $match: { tail_num: { $ne: null, $ne: "", $exists: true } } },
    { $group: {
        _id: "$tail_num",
        first_flight_year: { $min: "$year" },
        last_flight_year: { $max: "$year" },
        total_flights: { $sum: 1 }
    } },
    { $addFields: {
        tail_num: "$_id"
    } },
    { $project: {
        _id: 0,
        tail_num: 1,
        first_flight_year: 1,
        last_flight_year: 1,
        total_flights: 1
    } },
    { $out: "aircraft_temp" }
]);

// Compute primary airline per aircraft 
const aircraftWithPrimaryAirline = db.flights.aggregate([
    { $match: { tail_num: { $ne: null, $ne: "", $exists: true } } },
    { $group: {
        _id: {
            tail_num: "$tail_num",
            carrier: "$carrier.code"
        },
        count: { $sum: 1 }
    } },
    { $sort: { "_id.tail_num": 1, count: -1 } },
    { $group: {
        _id: "$_id.tail_num",
        primary_airline: { $first: "$_id.carrier" }
    } }
]).toArray();

// Create a lookup map
const primaryAirlineMap = {};
aircraftWithPrimaryAirline.forEach(a => {
    primaryAirlineMap[a._id] = a.primary_airline;
});

// Update aircraft_temp with primary_airline
const aircraftToInsert = [];
const existingAircraft = db.aircraft_temp.find().toArray();

for (const aircraft of existingAircraft) {
    aircraftToInsert.push({
        tail_num: aircraft.tail_num,
        first_flight_year: aircraft.first_flight_year,
        last_flight_year: aircraft.last_flight_year,
        primary_airline: primaryAirlineMap[aircraft.tail_num] || null,
        total_flights: aircraft.total_flights
    });
}

// Clear and repopulate aircraft collection
db.aircraft.drop();
if (aircraftToInsert.length > 0) {
    db.aircraft.insertMany(aircraftToInsert);
}

// Drop temp collection
db.aircraft_temp.drop();

print(`   ✅ Created ${db.aircraft.countDocuments().toLocaleString()} aircraft documents`);

// Show sample
print("\n   Sample aircraft documents (matching relational schema):");
db.aircraft.find().limit(5).forEach(a => {
    print(`      - ${a.tail_num}: first=${a.first_flight_year}, last=${a.last_flight_year}, ` +
          `primary=${a.primary_airline || 'N/A'}, flights=${a.total_flights}`);
});

// CITIES Collection 
print("\n📝 2. Building cities collection (matching relational schema)...");
db.cities.drop();

// Process airports to extract unique cities with timezone info
const airports = db.airports.find({ 
    country: "United States",
    city: { $ne: null, $ne: "" }
}).toArray();

print(`   Processing ${airports.length} US airports...`);

const cityMap = new Map();

airports.forEach(airport => {
    let cityName = airport.city;
    
    // Clean city name (remove state code if present)
    if (cityName && cityName.includes(',')) {
        cityName = cityName.split(',')[0].trim();
    } else if (cityName && cityName.length > 2) {
        // Check if ends with 2-letter state code
        const words = cityName.split(' ');
        const lastWord = words[words.length - 1];
        if (lastWord.length === 2 && lastWord.match(/[A-Z]{2}/)) {
            cityName = words.slice(0, -1).join(' ');
        }
    }
    
    const key = cityName;
    
    if (!cityMap.has(key)) {
        cityMap.set(key, {
            city_name: cityName,
            country: airport.country || "United States",
            timezone_offset: airport.timezone_offset,
            dst: airport.dst,
            tz_database: airport.tz_database
        });
    } else {
        // Update with better timezone info if available
        const existing = cityMap.get(key);
        if (!existing.tz_database && airport.tz_database) {
            existing.tz_database = airport.tz_database;
        }
        if (!existing.timezone_offset && airport.timezone_offset) {
            existing.timezone_offset = airport.timezone_offset;
        }
    }
});

const citiesToInsert = [];
for (const [_, cityData] of cityMap) {
    citiesToInsert.push({
        city_name: cityData.city_name,
        country: cityData.country,
        timezone_offset: cityData.timezone_offset,
        dst: cityData.dst,
        tz_database: cityData.tz_database
    });
}

if (citiesToInsert.length > 0) {
    db.cities.insertMany(citiesToInsert);
}

print(`   ✅ Created ${db.cities.countDocuments().toLocaleString()} city documents`);
print(`   Fields: city_name, country, timezone_offset, dst, tz_database`);

print("\n   Sample cities:");
db.cities.find().limit(5).forEach(city => {
    print(`      - ${city.city_name}, ${city.country} (tz: ${city.tz_database || 'N/A'})`);
});

// Create Indexes for these documents
print("\n📝 3. Creating indexes...");

db.aircraft.createIndex({ tail_num: 1 }, { unique: true });
db.aircraft.createIndex({ primary_airline: 1 });
db.aircraft.createIndex({ first_flight_year: 1 });
db.aircraft.createIndex({ last_flight_year: 1 });
db.cities.createIndex({ city_name: 1 }, { unique: true });
db.cities.createIndex({ country: 1 });

print("   ✅ All indexes created");

// Summary
print("\n" + "=".repeat(60));
print("✅ DERIVED COLLECTIONS POPULATION COMPLETE");
print("=".repeat(60));

print("\n📊 Final counts (matching relational schema):");
print(`   - aircraft: ${db.aircraft.countDocuments().toLocaleString()}`);
print(`     Fields: tail_num, first_flight_year, last_flight_year, primary_airline, total_flights`);
print(`   - cities: ${db.cities.countDocuments().toLocaleString()}`);
print(`     Fields: city_name, country, timezone_offset, dst, tz_database`);

print("\n📋 Schema verification:");
const sampleAircraft = db.aircraft.findOne();
if (sampleAircraft) {
    const fields = Object.keys(sampleAircraft).filter(k => k !== '_id');
    print(`   - Aircraft fields: ${fields.join(', ')}`);
}

const sampleCity = db.cities.findOne();
if (sampleCity) {
    const fields = Object.keys(sampleCity).filter(k => k !== '_id');
    print(`   - Cities fields: ${fields.join(', ')}`);
}

print("\n💡 MongoDB collections now match PostgreSQL relational schema exactly!");
print("=".repeat(60));