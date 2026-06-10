r"""
PREPROCESSING SCRIPT — Flight Data Analysis Project 
Streams the flights CSV in chunks. Uses orjson for Mongo NDJSON output.

Outputs:
  data/output/airports.csv
  data/output/airlines.csv
  data/output/flights.csv                  
  data/output/flights_mongo_XXXX.ndjson    
  data/docs/data_quality_report.json
"""

import pandas as pd
import numpy as np
import orjson
import json
import logging
import re
import signal
import sys
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─ Paths ─
RAW_DIR   = Path("data/raw")
CLEAN_DIR = Path("data/clean")
OUT_DIR   = Path("data/output")
DOCS_DIR  = Path("data/docs")

for d in [RAW_DIR, CLEAN_DIR, OUT_DIR, DOCS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

FLIGHTS_CSV  = RAW_DIR / "airline.csv.shuffle"
AIRPORTS_DAT = RAW_DIR / "airports.dat"
AIRLINES_DAT = RAW_DIR / "airlines.dat"

CHUNK_SIZE = 500_000

CHECKPOINT_FILE = DOCS_DIR / "pipeline_checkpoint.json"

_interrupted = False

def _handle_signal(sig, frame):
    global _interrupted
    logger.warning("Interrupt received — will stop after current chunk and save checkpoint.")
    _interrupted = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            cp = json.load(f)
        logger.info(f"Resuming from checkpoint: {CHECKPOINT_FILE}")
        logger.info(f"  scan_done={cp.get('scan_done')}, "
                    f"chunk_idx={cp.get('chunk_idx', 0)}, "
                    f"flight_id={cp.get('flight_id', 0)}, "
                    f"total_kept={cp.get('total_kept', 0)}")
        return cp
    return {}


def save_checkpoint(cp: dict) -> None:
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(cp, f, indent=2)
    tmp.replace(CHECKPOINT_FILE)
    logger.info(f"Checkpoint saved → chunk_idx={cp.get('chunk_idx', 0)}, "
                f"flight_id={cp.get('flight_id', 0)}, "
                f"total_kept={cp.get('total_kept', 0)}")


def clear_checkpoint() -> None:
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("Checkpoint cleared (pipeline complete).")

SAMPLE_FRAC  = 0.02        # sample 2% of all the flights
SAMPLE_YEARS = None        

RANDOM_SEED = 42

# COLUMN RENAMING
RENAME_MAP = {
    "ActualElapsedTime": "actual_elapsed_time",
    "AirTime": "air_time",
    "ArrDelay": "arr_delay",
    "ArrTime": "arr_time",
    "CRSArrTime": "crs_arr_time",
    "CRSDepTime": "crs_dep_time",
    "CRSElapsedTime": "crs_elapsed_time",
    "CancellationCode": "cancellation_code",
    "Cancelled": "cancelled",
    "CarrierDelay": "carrier_delay",
    "DayOfWeek": "day_of_week",
    "DayofMonth": "day_of_month",
    "DepDelay": "dep_delay",
    "DepTime": "dep_time",
    "Dest": "dest",
    "Distance": "distance",
    "Diverted": "diverted",
    "FlightNum": "flight_num",
    "LateAircraftDelay": "late_aircraft_delay",
    "Month": "month",
    "NASDelay": "nas_delay",
    "Origin": "origin",
    "SecurityDelay": "security_delay",
    "TailNum": "tail_num",
    "TaxiIn": "taxi_in",
    "TaxiOut": "taxi_out",
    "UniqueCarrier": "unique_carrier",
    "WeatherDelay": "weather_delay",
    "Year": "year",
}

def _to_snake_case(name: str) -> str:
    return RENAME_MAP.get(name, name.lower())

# COLUMN TYPE HINTS

INT_COLS = [
    "year", "month", "day_of_month", "day_of_week",
    "actual_elapsed_time", "air_time", "arr_time",
    "crs_arr_time", "crs_dep_time", "crs_elapsed_time",
    "dep_time", "distance", "flight_num",
    "taxi_in", "taxi_out",
    "arr_delay", "dep_delay",
]

FLOAT_COLS = [
    "carrier_delay", "weather_delay", "nas_delay",
    "security_delay", "late_aircraft_delay",
]

NUMERIC_COLS = INT_COLS + FLOAT_COLS

# Time columns that BTS encodes as HHMM.
CRS_TIME_COLS    = ["crs_dep_time", "crs_arr_time"]
ACTUAL_TIME_COLS = ["dep_time", "arr_time"]
TIME_COLS_HHMM   = CRS_TIME_COLS + ACTUAL_TIME_COLS

# Valid US (FAA) tail number pattern: N + 1-5 digits/letters
TAIL_NUM_RE = re.compile(r"^N[A-Za-z0-9]{1,5}$")

# AIRPORTS
def load_airports(path: Path) -> pd.DataFrame:
    logger.info(f"Loading airports from {path} ...")

    col_names = [
        "airport_id", "name", "city", "country",
        "iata_code", "icao_code",
        "latitude", "longitude", "altitude",
        "timezone_offset", "dst", "tz_database",
        "type", "source"
    ]

    df = pd.read_csv(path, header=None, names=col_names,
                     na_values=["\\N", ""], quotechar='"')
    logger.info(f"  Raw rows: {len(df):,}")

    df = df[df["iata_code"].notna()].copy()
    df["iata_code"] = df["iata_code"].str.strip().str.upper()
    df = df[df["iata_code"].str.match(r"^[A-Z]{3}$", na=False)]
    df = df[df["type"] == "airport"]

    us_territories = [
        "United States", "Puerto Rico", "United States Virgin Islands",
        "Guam", "American Samoa", "Northern Mariana Islands",
        "Virgin Islands", "Palau", "Micronesia",
    ]
    df = df[df["country"].isin(us_territories)].copy()
    logger.info(f"  US airports with valid IATA: {len(df):,}")

    df = df.sort_values("source", ascending=False)
    df = df.drop_duplicates(subset="iata_code", keep="first")
    logger.info(f"  After dedup: {len(df):,}")
    return df

# AIRLINES
def load_airlines(path: Path) -> pd.DataFrame:
    logger.info(f"Loading airlines from {path} ...")

    col_names = [
        "airline_id", "name", "alias",
        "iata_code", "icao_code", "callsign",
        "country", "active"
    ]
    df = pd.read_csv(path, header=None, names=col_names,
                     na_values=["\\N"], quotechar='"')
    logger.info(f"  Raw rows: {len(df):,}")

    df["iata_code"] = df["iata_code"].replace({"-": pd.NA, "": pd.NA})
    df["iata_code"] = df["iata_code"].str.strip().str.upper()

    df = df[df["iata_code"].notna()].copy()
    df = df[df["iata_code"].str.match(r"^[A-Z0-9]{2,3}$", na=False)]

    df["active"] = df["active"].str.strip().str.upper()
    df["is_defunct"] = df["active"] != "Y"

    df = df.sort_values("is_defunct")
    df = df.drop_duplicates(subset="iata_code", keep="first")
    logger.info(f"  Airlines with valid IATA: {len(df):,}")
    return df

# CARRIER OVERRIDES
# Defunct carriers entirely missing from airlines.dat.
MANUAL_CARRIERS = {
    "TW": ("Trans World Airlines",            "United States"),
    "EA": ("Eastern Air Lines",               "United States"),
    "PA": ("Pan American World Airways",      "United States"),
    "PI": ("Piedmont Airlines",               "United States"),
    "PS": ("PSA (Pacific Southwest Airlines)", "United States"),
    "ML": ("Midway Airlines",                 "United States"),
    "DH": ("Independence Air",                "United States"),
    "TZ": ("ATA Airlines",                    "United States"),
}

# Per-field corrections applied AFTER the airlines.dat lookup, without
# overwriting other fields. Use this for fixing OpenFlights data quality
# issues (wrong country, wrong name, wrong defunct status).
# Each value is a dict of fields to override.
CARRIER_FIELD_OVERRIDES = {
    "AS": {"country_of_registration": "United States"},      # "ALASKA" → US
    "CO": {"airline_name": "Continental Airlines",            # not "Continental Express"
           "country_of_registration": "United States"},
}

# Set of known-valid country names. Anything in airlines.dat that isn't
# in here is flagged in the quality report as suspicious — useful for
# catching future data issues like the "ALASKA" bug.
KNOWN_COUNTRIES_SAMPLE = {
    "United States", "Canada", "Mexico", "United Kingdom", "France",
    "Germany", "Italy", "Spain", "Netherlands", "Ireland", "Japan",
    "China", "South Korea", "Australia", "New Zealand", "Brazil",
    "Argentina", "Chile", "Colombia", "Peru", "South Africa", "Egypt",
    "United Arab Emirates", "Qatar", "India", "Singapore", "Hong Kong",
    "Taiwan", "Thailand", "Vietnam", "Indonesia", "Malaysia",
    "Switzerland", "Austria", "Belgium", "Denmark", "Sweden", "Norway",
    "Finland", "Iceland", "Russia", "Turkey", "Greece", "Portugal",
    "Poland", "Czech Republic", "Hungary", "Romania", "Israel",
    "Saudi Arabia", "Morocco", "Kenya", "Ethiopia", "Nigeria",
    "Philippines", "Pakistan", "Bangladesh", "Sri Lanka",
}

# SAMPLING HELPER
def _apply_sampling(chunk: pd.DataFrame, year_col: str = "year") -> pd.DataFrame:
    if SAMPLE_YEARS is not None:
        chunk = chunk[chunk[year_col].isin(SAMPLE_YEARS)]
        if len(chunk) == 0:
            return chunk
    if SAMPLE_FRAC is not None and SAMPLE_FRAC < 1.0:
        chunk = chunk.sample(frac=SAMPLE_FRAC, random_state=RANDOM_SEED)
    return chunk

# FIRST PASS
def scan_flights(path: Path, cp: dict, chunk_size: int = CHUNK_SIZE) -> dict:
    """First pass: collect unique carrier/airport codes.

    If ``cp`` contains a completed scan (``scan_done=True``) the result is
    returned immediately from the checkpoint without re-reading the file.
    Otherwise the scan runs from scratch and the result is stored in ``cp``
    so a subsequent resume can skip this phase entirely.
    """
    if cp.get("scan_done"):
        logger.info("Scan already complete — loading from checkpoint.")
        return {
            "carriers": set(cp["scan_carriers"]),
            "airports": set(cp["scan_airports"]),
            "carrier_flight_counts": cp["scan_carrier_counts"],
            "airport_flight_counts": cp["scan_airport_counts"],
            "total_rows": cp["scan_total_rows"],
            "total_rows_in_sample": cp["scan_total_sampled"],
        }

    logger.info(f"First pass: scanning {path} for unique codes ...")

    carrier_counts = {}
    airport_counts = {}
    total = 0
    total_sampled = 0

    usecols = ["UniqueCarrier", "Origin", "Dest", "Year"]
    reader = pd.read_csv(
        path, usecols=usecols,
        chunksize=chunk_size, low_memory=False, encoding="latin-1",
    )

    for i, chunk in enumerate(reader):
        chunk.columns = [_to_snake_case(c) for c in chunk.columns]
        total += len(chunk)

        chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce")
        chunk = _apply_sampling(chunk, "year")
        if len(chunk) == 0:
            if _interrupted:
                # Save partial scan progress so we can at least skip the
                # chunks already read next time (scan restarts from scratch,
                # but the outer loop will re-enter here).
                logger.warning("Interrupted during scan — partial scan NOT saved; "
                               "scan will restart on next run.")
                sys.exit(1)
            continue

        total_sampled += len(chunk)

        chunk["unique_carrier"] = (
            chunk["unique_carrier"].astype(str).str.strip()
            .str.replace(r"\s*\(\d+\)$", "", regex=True)
            .str.upper()
        )
        chunk["origin"] = chunk["origin"].astype(str).str.strip().str.upper()
        chunk["dest"]   = chunk["dest"].astype(str).str.strip().str.upper()

        for code, n in chunk["unique_carrier"].value_counts().items():
            carrier_counts[code] = carrier_counts.get(code, 0) + int(n)
        for code, n in chunk["origin"].value_counts().items():
            airport_counts[code] = airport_counts.get(code, 0) + int(n)
        for code, n in chunk["dest"].value_counts().items():
            airport_counts[code] = airport_counts.get(code, 0) + int(n)

        if (i + 1) % 20 == 0:
            logger.info(f"    Scanned {total:,} rows "
                        f"({total_sampled:,} in sample) ...")

        if _interrupted:
            logger.warning("Interrupted during scan — partial scan NOT saved; "
                           "scan will restart on next run.")
            sys.exit(1)

    logger.info(f"  Done. Total rows read: {total:,}, in sample: {total_sampled:,}")
    logger.info(f"  Unique carriers: {len(carrier_counts)}")
    logger.info(f"  Unique airports: {len(airport_counts)}")

    # Persist scan results so next resume skips this phase
    cp["scan_done"] = True
    cp["scan_carriers"] = sorted(carrier_counts)
    cp["scan_airports"] = sorted(airport_counts)
    cp["scan_carrier_counts"] = carrier_counts
    cp["scan_airport_counts"] = airport_counts
    cp["scan_total_rows"] = total
    cp["scan_total_sampled"] = total_sampled
    save_checkpoint(cp)

    return {
        "carriers": set(carrier_counts),
        "airports": set(airport_counts),
        "carrier_flight_counts": carrier_counts,
        "airport_flight_counts": airport_counts,
        "total_rows": total,
        "total_rows_in_sample": total_sampled,
    }


def match_airports(scan: dict, airports: pd.DataFrame) -> dict:
    airport_codes = set(airports["iata_code"])
    flight_codes  = scan["airports"]
    matched   = flight_codes & airport_codes
    unmatched = flight_codes - airport_codes
    unmatched_counts = {c: scan["airport_flight_counts"][c] for c in sorted(unmatched)}

    report = {
        "total_unique_codes_in_flights": len(flight_codes),
        "matched": len(matched),
        "unmatched": len(unmatched),
        "match_rate_pct": round(len(matched) / max(len(flight_codes), 1) * 100, 2),
        "unmatched_codes_with_flight_counts": unmatched_counts,
    }
    logger.info(f"Airport match: {report['match_rate_pct']}% "
                f"({len(matched)}/{len(flight_codes)})")
    if unmatched:
        top = sorted(unmatched_counts.items(), key=lambda x: -x[1])[:15]
        logger.warning(f"  Top unmatched: {top}")
    return report


def match_carriers(scan: dict, airlines: pd.DataFrame) -> dict:
    airline_iata = set(airlines["iata_code"].dropna())
    flight_carriers = scan["carriers"]
    matched   = flight_carriers & airline_iata
    unmatched = flight_carriers - airline_iata
    unmatched_counts = {c: scan["carrier_flight_counts"][c] for c in sorted(unmatched)}

    report = {
        "total_unique_carriers_in_flights": len(flight_carriers),
        "matched_to_airlines_dat": len(matched),
        "unmatched": len(unmatched),
        "match_rate_pct": round(len(matched) / max(len(flight_carriers), 1) * 100, 2),
        "unmatched_carriers_with_flight_counts": unmatched_counts,
    }
    logger.info(f"Carrier match: {report['match_rate_pct']}% "
                f"({len(matched)}/{len(flight_carriers)})")
    if unmatched:
        logger.warning(f"  Unmatched carriers: {sorted(unmatched)}")
    return report

# BUILD CONSOLIDATED AIRLINES TABLE
def build_airlines_table(scan: dict, airlines: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve each carrier seen in flights → name + country + defunct status.

    Resolution priority:
      1. MANUAL_CARRIERS   (defunct carriers missing from airlines.dat)
      2. airlines.dat lookup
      3. unresolved (will not be filtered out — kept with empty fields)

    After lookup, CARRIER_FIELD_OVERRIDES corrects per-field issues
    (e.g. AS country = "ALASKA" → "United States", CO name correction).

    is_defunct is now nullable: True / False / None.
      - "manual" entries are known defunct → True
      - airlines.dat with active="Y" → False
      - airlines.dat with active="N" → True
      - airlines.dat with active missing/other → None (unknown)
      - unresolved → None (unknown)

    """
    all_carriers = sorted(scan["carriers"])
    rows = []

    for code in all_carriers:
        if code in MANUAL_CARRIERS:
            name, country = MANUAL_CARRIERS[code]
            row = {
                "unique_carrier": code,
                "airline_name": name,
                "country_of_registration": country,
                "icao_code": "",
                "is_defunct": True,
                "source": "manual",
            }
        else:
            match = airlines[airlines["iata_code"] == code]
            if not match.empty:
                # Prefer the active, non-aliased entry if there are duplicates.
                # airlines.dat is already deduplicated by iata_code in
                # load_airlines (active first via sort_values), so iloc[0]
                # already gives us the best candidate.
                r = match.iloc[0]
                # Tri-state defunct: check raw active field
                active = (str(r.get("active", "")).strip().upper()
                          if pd.notna(r.get("active")) else "")
                if active == "Y":
                    is_defunct = False
                elif active == "N":
                    is_defunct = True
                else:
                    is_defunct = None  # unknown
                row = {
                    "unique_carrier": code,
                    "airline_name": r["name"] if pd.notna(r["name"]) else "",
                    "country_of_registration":
                        r["country"] if pd.notna(r.get("country")) else "",
                    "icao_code":
                        r["icao_code"] if pd.notna(r.get("icao_code")) else "",
                    "is_defunct": is_defunct,
                    "source": "airlines_dat",
                }
            else:
                row = {
                    "unique_carrier": code,
                    "airline_name": "",
                    "country_of_registration": "",
                    "icao_code": "",
                    "is_defunct": None,  # unknown — was False before
                    "source": "unresolved",
                }

        # Apply per-field overrides (only the fields specified)
        if code in CARRIER_FIELD_OVERRIDES:
            for field, value in CARRIER_FIELD_OVERRIDES[code].items():
                row[field] = value
            row["source"] = row["source"] + "+override"

        rows.append(row)

    result = pd.DataFrame(rows)

    # Logging by resolution source
    for src_pattern in ["airlines_dat", "manual", "unresolved"]:
        n = result["source"].str.startswith(src_pattern).sum()
        logger.info(f"  Carrier resolution - {src_pattern}*: {n}")

    return result


def audit_carriers(airlines_table: pd.DataFrame) -> dict:
    """Flag suspicious country values for the quality report."""
    audit = {"suspicious_countries": [], "international_carriers": []}
    for _, r in airlines_table.iterrows():
        country = r["country_of_registration"]
        code = r["unique_carrier"]
        if not country:
            continue
        if country not in KNOWN_COUNTRIES_SAMPLE:
            audit["suspicious_countries"].append({
                "code": code, "name": r["airline_name"], "country": country,
            })
        elif country != "United States":
            audit["international_carriers"].append({
                "code": code, "name": r["airline_name"], "country": country,
            })
    if audit["suspicious_countries"]:
        logger.warning(f"  Suspicious carrier countries: "
                       f"{audit['suspicious_countries']}")
    if audit["international_carriers"]:
        logger.info(f"  International carriers operating US flights: "
                    f"{[c['code'] for c in audit['international_carriers']]}")
    return audit


# CLEAN A FLIGHTS CHUNK
def clean_flights_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [_to_snake_case(c) for c in df.columns]

    # Numeric coercion
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # HHMM time columns
    for col in TIME_COLS_HHMM:
        if col not in df.columns:
            continue
        # Normalize 2400 → 0 
        df[col] = df[col].where(df[col] != 2400, 0)
        # Invalid values: out of range or minutes field >= 60
        hh = df[col] // 100
        mm = df[col] % 100
        invalid = (df[col] < 0) | (df[col] > 2359) | (mm >= 60) | (hh >= 24)
        df[col] = df[col].where(~invalid, pd.NA)

    for col in CRS_TIME_COLS:
        if col in df.columns:
            df[col] = df[col].where(df[col] != 0, pd.NA)

    cancelled_or_diverted = df["cancelled"] | df["diverted"]
    for col in ACTUAL_TIME_COLS:
        if col in df.columns:
            mask = (df[col] == 0) & cancelled_or_diverted
            df[col] = df[col].where(~mask, pd.NA)

    # Integer columns 
    for col in INT_COLS:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    # Booleans
    df["cancelled"] = df["cancelled"].astype(int).astype(bool)
    df["diverted"]  = df["diverted"].astype(int).astype(bool)

    # Cancellation code
    df["cancellation_code"] = df["cancellation_code"].replace({"NA": pd.NA, "": pd.NA})

    # String columns
    df["origin"] = df["origin"].astype(str).str.strip().str.upper()
    df["dest"]   = df["dest"].astype(str).str.strip().str.upper()
    df["unique_carrier"] = (
        df["unique_carrier"].astype(str).str.strip()
        .str.replace(r"\s*\(\d+\)$", "", regex=True)
        .str.upper()
    )

    # Tail num: validate against FAA pattern, set invalid to NA.
    df["tail_num"] = df["tail_num"].replace({"NA": pd.NA})
    tail_str = df["tail_num"].astype(str).str.strip().str.upper()
    valid_tail = tail_str.str.match(TAIL_NUM_RE, na=False)
    df["tail_num"] = tail_str.where(valid_tail, pd.NA)

    return df

# MONGO LOOKUP DICTS
def build_lookups(airports: pd.DataFrame, airlines_table: pd.DataFrame):
    airport_lookup = {}
    for _, r in airports.iterrows():
        lon = float(r["longitude"]) if pd.notna(r.get("longitude")) else None
        lat = float(r["latitude"])  if pd.notna(r.get("latitude"))  else None
        airport_lookup[r["iata_code"]] = {
            "iata": r["iata_code"],
            "name": r.get("name", "") or "",
            "city": r.get("city", "") or "",
            "country": r.get("country", "") or "",
            "location": {"type": "Point", "coordinates": [lon, lat]},
        }

    carrier_lookup = {}
    for _, r in airlines_table.iterrows():
        defunct = r.get("is_defunct")
        if pd.isna(defunct):
            defunct = None
        else:
            defunct = bool(defunct)
        carrier_lookup[r["unique_carrier"]] = {
            "code": r["unique_carrier"],
            "name": r.get("airline_name", "") or "",
            "country": r.get("country_of_registration", "") or "",
            "is_defunct": defunct,
        }

    return airport_lookup, carrier_lookup

# BUILD AIRCRAFT SUMMARY LOOKUP 
def build_aircraft_lookup(flights_path: Path, chunk_size: int = CHUNK_SIZE) -> dict:
    """First pass: build aircraft summary statistics to embed in each flight."""
    logger.info("Building aircraft summary lookup (first pass)...")
    
    aircraft_stats = {}
    carrier_counts_per_aircraft = defaultdict(lambda: defaultdict(int))
    
    reader = pd.read_csv(
        flights_path, 
        usecols=["TailNum", "UniqueCarrier", "Year"],
        chunksize=chunk_size, 
        low_memory=False, 
        encoding="latin-1"
    )
    
    for i, chunk in enumerate(reader):
        chunk.columns = [_to_snake_case(c) for c in chunk.columns]
        
        # Track per-aircraft stats
        for _, row in chunk.iterrows():
            tail = row.get("tail_num")
            if pd.isna(tail) or tail == "":
                continue
                
            tail = str(tail).strip().upper()
            year = row.get("year")
            carrier = row.get("unique_carrier")
            
            if tail not in aircraft_stats:
                aircraft_stats[tail] = {
                    "first_flight_year": year,
                    "last_flight_year": year,
                    "total_flights": 0
                }
            else:
                if year < aircraft_stats[tail]["first_flight_year"]:
                    aircraft_stats[tail]["first_flight_year"] = year
                if year > aircraft_stats[tail]["last_flight_year"]:
                    aircraft_stats[tail]["last_flight_year"] = year
            
            aircraft_stats[tail]["total_flights"] += 1
            
            if carrier and not pd.isna(carrier):
                carrier_counts_per_aircraft[tail][str(carrier).strip().upper()] += 1
        
        if (i + 1) % 20 == 0:
            logger.info(f"    Scanned {i+1} chunks for aircraft stats...")
        
        if _interrupted:
            sys.exit(1)
    
    # Determine primary airline for each aircraft
    for tail, carriers in carrier_counts_per_aircraft.items():
        if carriers:
            primary = max(carriers, key=carriers.get)
            aircraft_stats[tail]["primary_airline"] = primary
        else:
            aircraft_stats[tail]["primary_airline"] = None
    
    logger.info(f"  Built lookup for {len(aircraft_stats):,} aircraft")
    return aircraft_stats

# BUILD CITY LOOKUP (to nest inside airports)
def build_city_lookup(airports: pd.DataFrame) -> dict:
    """Build city lookup to nest inside airport objects."""
    logger.info("Building city lookup for nesting in airports...")
    
    city_lookup = {}
    
    for _, r in airports.iterrows():
        iata = r["iata_code"]
        city_name = r.get("city", "")
        
        # Clean city name
        if city_name and isinstance(city_name, str):
            if ',' in city_name:
                city_name = city_name.split(',')[0].strip()
        
        city_info = {
            "city_name": city_name or None,
            "country": r.get("country", "United States") if pd.notna(r.get("country")) else "United States",
            "timezone_offset": float(r["timezone_offset"]) if pd.notna(r.get("timezone_offset")) else None,
            "dst": r.get("dst") if pd.notna(r.get("dst")) else None,
            "tz_database": r.get("tz_database") if pd.notna(r.get("tz_database")) else None
        }
        
        city_lookup[iata] = city_info
    
    logger.info(f"  Built city lookup for {len(city_lookup):,} airports")
    return city_lookup


# MODIFIED MONGO DOCUMENT BUILDER (with embedded aircraft, delays, city)
def write_mongo_chunk(chunk: pd.DataFrame, airport_lookup: dict,
                      carrier_lookup: dict, aircraft_lookup: dict,
                      city_lookup: dict, chunk_idx: int) -> int:
    """Write NDJSON with ALL data denormalized into flights collection."""
    out_file = OUT_DIR / f"flights_mongo_{chunk_idx:04d}.ndjson"
    records = chunk.to_dict(orient="records")

    newline = b"\n"
    with open(out_file, "wb") as f:
        for row in records:
            origin_iata = row.get("origin")
            dest_iata = row.get("dest")
            tail_num = row.get("tail_num")
            
            # Get airport objects with ALL fields including coordinates
            origin_airport = airport_lookup.get(origin_iata, {"iata": origin_iata})
            dest_airport = airport_lookup.get(dest_iata, {"iata": dest_iata})
            
            # Add nested city info to airports
            if origin_iata and origin_iata in city_lookup:
                origin_airport["city"] = city_lookup[origin_iata]
            if dest_iata and dest_iata in city_lookup:
                dest_airport["city"] = city_lookup[dest_iata]
            
            # Get aircraft summary info
            aircraft_info = None
            if tail_num and tail_num in aircraft_lookup:
                ac = aircraft_lookup[tail_num]
                aircraft_info = {
                    "tail_num": tail_num,
                    "first_flight_year": ac["first_flight_year"],
                    "last_flight_year": ac["last_flight_year"],
                    "primary_airline": ac.get("primary_airline"),
                    "total_flights": ac["total_flights"]
                }
            
            # Build complete flight document
            doc = {
                "flight_id": _int(row.get("flight_id")),
                "year": _int(row.get("year")),
                "month": _int(row.get("month")),
                "day_of_month": _int(row.get("day_of_month")),
                "day_of_week": _int(row.get("day_of_week")),

                "carrier": carrier_lookup.get(
                    row.get("unique_carrier"), {"code": row.get("unique_carrier")}
                ),
                "origin": origin_airport,
                "dest": dest_airport,

                "aircraft": aircraft_info,

                "schedule": {
                    "crs_dep_time": _int(row.get("crs_dep_time")),
                    "crs_arr_time": _int(row.get("crs_arr_time")),
                    "crs_elapsed_time": _int(row.get("crs_elapsed_time")),
                },
                "actual": {
                    "dep_time": _int(row.get("dep_time")),
                    "arr_time": _int(row.get("arr_time")),
                    "elapsed_time": _int(row.get("actual_elapsed_time")),
                    "air_time": _int(row.get("air_time")),
                    "taxi_in": _int(row.get("taxi_in")),
                    "taxi_out": _int(row.get("taxi_out")),
                },
                
                "delays": {
                    "delay_id": _int(row.get("flight_id")),
                    "dep_delay": _int(row.get("dep_delay")),
                    "arr_delay": _int(row.get("arr_delay")),
                    "carrier_delay": _float(row.get("carrier_delay")),
                    "weather_delay": _float(row.get("weather_delay")),
                    "nas_delay": _float(row.get("nas_delay")),
                    "security_delay": _float(row.get("security_delay")),
                    "late_aircraft_delay": _float(row.get("late_aircraft_delay")),
                },

                "flight_num": _int(row.get("flight_num")),
                "tail_num": _str(tail_num),
                "distance": _int(row.get("distance")),
                "cancelled": bool(row.get("cancelled", False)),
                "diverted": bool(row.get("diverted", False)),
                "cancellation_code": _str(row.get("cancellation_code")),
            }
            f.write(orjson.dumps(doc))
            f.write(newline)

    return len(records)

# STREAMING PIPELINE
def process_in_chunks(path: Path, airports: pd.DataFrame,
                      airlines_table: pd.DataFrame,
                      cp: dict,
                      chunk_size: int = CHUNK_SIZE) -> dict:
    logger.info(f"Streaming flights from {path} (chunk_size={chunk_size:,}) ...")

    # Build all lookup dictionaries
    valid_airports = set(airports["iata_code"])
    valid_carriers = set(airlines_table["unique_carrier"])
    airport_lookup, carrier_lookup = build_lookups(airports, airlines_table)
    
    # NEW: Build aircraft summary lookup (first pass through flights data)
    aircraft_lookup = build_aircraft_lookup(path, chunk_size)
    
    # NEW: Build city lookup from airports
    city_lookup = build_city_lookup(airports)

    # Resume state from checkpoint
    resume_from_chunk = cp.get("chunk_idx", 0)
    flight_id         = cp.get("flight_id", 0)
    total_read        = cp.get("total_read", 0)
    total_kept        = cp.get("total_kept", 0)
    total_invalid_tail             = cp.get("total_invalid_tail", 0)
    total_crs_time_zero            = cp.get("total_crs_time_zero", 0)
    total_actual_time_zero_on_nonop = cp.get("total_actual_time_zero_on_nonop", 0)
    total_invalid_hhmm             = cp.get("total_invalid_hhmm", 0)
    chunk_idx         = resume_from_chunk
    year_counts       = {int(k): v for k, v in cp.get("year_counts", {}).items()}
    year_with_delay   = {int(k): v for k, v in cp.get("year_with_delay", {}).items()}

    if resume_from_chunk > 0:
        logger.info(f"Resuming streaming from chunk {resume_from_chunk} "
                    f"(flight_id={flight_id:,}, total_kept={total_kept:,}) ...")

    # Open flights.csv for append when resuming, write (truncate) otherwise
    pg_path = OUT_DIR / "flights.csv"
    open_mode = "a" if resume_from_chunk > 0 and pg_path.exists() else "w"
    first_write = (open_mode == "w")
    pg_writer = open(pg_path, open_mode, encoding="utf-8", newline="")

    delay_cols = ["carrier_delay", "weather_delay", "nas_delay",
                  "security_delay", "late_aircraft_delay"]

    reader = pd.read_csv(path, chunksize=chunk_size, low_memory=False,
                         encoding="latin-1")

    try:
        for i, chunk in enumerate(reader):
            # Skip chunks already written in a previous run
            if i < resume_from_chunk:
                total_read += len(chunk)
                continue

            total_read += len(chunk)

            # Track tail nums BEFORE cleaning, to count nullified ones
            raw_tail = chunk["TailNum"] if "TailNum" in chunk.columns else None

            # Track time-zero rows BEFORE cleaning
            if "CRSDepTime" in chunk.columns:
                crs = pd.to_numeric(chunk["CRSDepTime"], errors="coerce")
                total_crs_time_zero += int((crs == 0).sum())
            if "DepTime" in chunk.columns and "Cancelled" in chunk.columns:
                dep = pd.to_numeric(chunk["DepTime"], errors="coerce")
                canc = pd.to_numeric(chunk["Cancelled"], errors="coerce").fillna(0).astype(int)
                div  = pd.to_numeric(chunk.get("Diverted", 0), errors="coerce").fillna(0).astype(int)
                total_actual_time_zero_on_nonop += int(((dep == 0) & ((canc + div) > 0)).sum())
            if "CRSDepTime" in chunk.columns:
                crs = pd.to_numeric(chunk["CRSDepTime"], errors="coerce")
                invalid = (crs == 2400) | (crs < 0) | (crs > 2359) | ((crs % 100) >= 60)
                total_invalid_hhmm += int(invalid.sum())

            chunk = clean_flights_chunk(chunk)

            # Count invalidated tail nums
            if raw_tail is not None:
                raw_clean = raw_tail.replace({"NA": pd.NA}).astype(str).str.strip().str.upper()
                had_value = raw_clean.notna() & (raw_clean != "NAN") & (raw_clean != "")
                still_valid = chunk["tail_num"].notna()
                total_invalid_tail += int((had_value & ~still_valid).sum())

            # Apply sampling
            chunk = _apply_sampling(chunk, "year")
            if len(chunk) == 0:
                if (i + 1) % 20 == 0:
                    logger.info(f"    Processed {total_read:,} read / "
                                f"{total_kept:,} kept ({chunk_idx} chunks) ...")
                continue

            # Referential integrity
            mask = (
                chunk["origin"].isin(valid_airports)
                & chunk["dest"].isin(valid_airports)
                & chunk["unique_carrier"].isin(valid_carriers)
            )
            chunk = chunk[mask].copy()
            if len(chunk) == 0:
                continue

            # Add flight_id
            chunk.insert(
                0, "flight_id",
                pd.array(
                    np.arange(flight_id + 1, flight_id + 1 + len(chunk), dtype=np.int64),
                    dtype="Int64",
                ),
            )
            flight_id += len(chunk)

            # Track delay availability for reporting
            has_delay = chunk[delay_cols].notna().any(axis=1)
            for y in chunk["year"].dropna().unique():
                y_int = int(y)
                mask_y = (chunk["year"] == y)
                year_counts[y_int] = year_counts.get(y_int, 0) + int(mask_y.sum())
                year_with_delay[y_int] = year_with_delay.get(y_int, 0) + int(
                    (has_delay & mask_y).sum()
                )

            # PG export (keep for compatibility)
            pg_out = chunk.copy()
            pg_out["cancelled"] = pg_out["cancelled"].map({True: "t", False: "f"})
            pg_out["diverted"]  = pg_out["diverted"].map({True: "t", False: "f"})
            pg_out.to_csv(pg_writer, header=first_write, index=False, na_rep="")
            first_write = False

            # MODIFIED: Write MongoDB NDJSON with ALL embedded data
            write_mongo_chunk(chunk, airport_lookup, carrier_lookup, 
                            aircraft_lookup, city_lookup, chunk_idx)

            total_kept += len(chunk)
            chunk_idx += 1

            if chunk_idx % 5 == 0:
                logger.info(f"    Processed {total_read:,} read / "
                            f"{total_kept:,} kept ({chunk_idx} chunks) ...")

            # Save checkpoint after every completed chunk
            cp.update({
                "chunk_idx": chunk_idx,
                "flight_id": flight_id,
                "total_read": total_read,
                "total_kept": total_kept,
                "total_invalid_tail": total_invalid_tail,
                "total_crs_time_zero": total_crs_time_zero,
                "total_actual_time_zero_on_nonop": total_actual_time_zero_on_nonop,
                "total_invalid_hhmm": total_invalid_hhmm,
                "year_counts": year_counts,
                "year_with_delay": year_with_delay,
            })
            save_checkpoint(cp)

            if _interrupted:
                logger.warning("Interrupted — checkpoint saved. Re-run the script to resume.")
                pg_writer.close()
                sys.exit(0)

    except Exception:
        pg_writer.close()
        raise

    pg_writer.close()

    delay_availability = {
        y: round(year_with_delay.get(y, 0) / year_counts[y] * 100, 2)
        for y in sorted(year_counts)
    }

    logger.info(f"  Done. Read {total_read:,}, kept {total_kept:,}, "
                f"dropped {total_read - total_kept:,}")
    logger.info(f"  Invalid tail nums nullified (across full dataset): "
                f"{total_invalid_tail:,}")
    logger.info(f"  crs_dep_time=0 nullified:              {total_crs_time_zero:,}")
    logger.info(f"  dep_time=0 on cancelled/diverted:      {total_actual_time_zero_on_nonop:,}")
    logger.info(f"  Invalid HHMM values in crs_dep_time:   {total_invalid_hhmm:,}")
    logger.info(f"  flights.csv:  {pg_path}")
    logger.info(f"  Mongo NDJSON: {OUT_DIR}/flights_mongo_*.ndjson "
                f"({chunk_idx} files)")

    return {
        "total_read": total_read,
        "total_kept": total_kept,
        "dropped": total_read - total_kept,
        "mongo_chunks": chunk_idx,
        "delay_availability_pct_by_year": delay_availability,
        "invalid_tail_nums_nullified": total_invalid_tail,
        "crs_time_zero_nullified": total_crs_time_zero,
        "actual_time_zero_on_nonop_nullified": total_actual_time_zero_on_nonop,
        "invalid_hhmm_values": total_invalid_hhmm,
    }

# EXPORT DIMENSION TABLES
def export_dim_tables(airports: pd.DataFrame,
                      airlines_table: pd.DataFrame) -> None:
    logger.info("Exporting dimension tables (airports, airlines) ...")

    airport_cols = [
        "iata_code", "icao_code", "name", "city", "country",
        "latitude", "longitude", "altitude",
        "timezone_offset", "dst", "tz_database",
    ]
    airports_out = airports[[c for c in airport_cols if c in airports.columns]]
    airports_out.to_csv(OUT_DIR / "airports.csv", index=False, na_rep="")
    logger.info(f"  airports.csv: {len(airports_out):,} rows")

    # Convert nullable bool to "t"/"f"/"" for Postgres
    airlines_out = airlines_table.copy()
    def _bool_to_pg(v):
        if v is None or pd.isna(v): return ""
        return "t" if bool(v) else "f"
    airlines_out["is_defunct"] = airlines_out["is_defunct"].map(_bool_to_pg)
    airlines_out.to_csv(OUT_DIR / "airlines.csv", index=False, na_rep="")
    logger.info(f"  airlines.csv: {len(airlines_out):,} rows")


# HELPERS
def _int(val):
    if val is None: return None
    try:
        if pd.isna(val): return None
    except (TypeError, ValueError):
        pass
    try: return int(val)
    except (TypeError, ValueError): return None

def _float(val):
    if val is None: return None
    try:
        if pd.isna(val): return None
    except (TypeError, ValueError):
        pass
    try: return float(val)
    except (TypeError, ValueError): return None

def _str(val):
    if val is None: return None
    try:
        if pd.isna(val): return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s and s.lower() != "nan" else None

# MAIN
def main():
    logger.info("=" * 60)
    logger.info("PREPROCESSING PIPELINE START (streaming)")
    logger.info(f"  SAMPLE_YEARS = "
                f"{sorted(SAMPLE_YEARS) if SAMPLE_YEARS else 'None (all years)'}")
    logger.info(f"  SAMPLE_FRAC  = "
                f"{SAMPLE_FRAC if SAMPLE_FRAC else 'None (100%)'}")
    logger.info("=" * 60)

    for f in [FLIGHTS_CSV, AIRPORTS_DAT, AIRLINES_DAT]:
        if not f.exists():
            logger.error(f"Missing: {f}")
            return

    cp = load_checkpoint()

    airports = load_airports(AIRPORTS_DAT)
    airlines = load_airlines(AIRLINES_DAT)

    scan = scan_flights(FLIGHTS_CSV, cp)
    airport_report = match_airports(scan, airports)
    carrier_report = match_carriers(scan, airlines)

    airlines_table = build_airlines_table(scan, airlines)
    carrier_audit = audit_carriers(airlines_table)

    export_dim_tables(airports, airlines_table)

    stream_stats = process_in_chunks(FLIGHTS_CSV, airports, airlines_table, cp)

    report = {
        "sampling": {
            "sample_years": sorted(SAMPLE_YEARS) if SAMPLE_YEARS else "all",
            "sample_frac":  SAMPLE_FRAC if SAMPLE_FRAC else 1.0,
            "random_seed":  RANDOM_SEED,
        },
        "flights": {
            "total_rows_read": stream_stats["total_read"],
            "total_rows_kept": stream_stats["total_kept"],
            "dropped_at_export": stream_stats["dropped"],
        },
        "data_quality_fixes": {
            "invalid_tail_nums_nullified": stream_stats["invalid_tail_nums_nullified"],
            "crs_time_zero_nullified": stream_stats["crs_time_zero_nullified"],
            "actual_time_zero_on_nonop_nullified":
                stream_stats["actual_time_zero_on_nonop_nullified"],
            "invalid_hhmm_values_nullified": stream_stats["invalid_hhmm_values"],
        },
        "delay_cause_availability_pct_by_year":
            stream_stats["delay_availability_pct_by_year"],
        "airport_matching": airport_report,
        "carrier_matching": carrier_report,
        "carrier_audit": carrier_audit,
        "mongo_chunks_written": stream_stats["mongo_chunks"],
    }
    with open(DOCS_DIR / "data_quality_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    clear_checkpoint()

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  PostgreSQL: {OUT_DIR}/airports.csv, airlines.csv, flights.csv")
    logger.info(f"  MongoDB:    {OUT_DIR}/flights_mongo_*.ndjson")
    logger.info(f"  Report:     {DOCS_DIR}/data_quality_report.json")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()