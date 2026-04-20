"""
OpenF1 → PostgreSQL Pipeline
Fetches data from all OpenF1 API endpoints and stores it in a PostgreSQL database.
"""

import requests
import psycopg2
from psycopg2.extras import execute_values
import json
import time
import logging
from datetime import datetime
from config import DB_CONFIG, API_BASE_URL, ENDPOINTS, REQUEST_DELAY

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("openf1_pipeline.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection():
    """Return a new psycopg2 connection."""
    return psycopg2.connect(**DB_CONFIG)


def create_tables(conn):
    """Create all tables if they don't exist yet."""
    ddl = """
    -- Car telemetry data (~3.7 Hz per car)
    CREATE TABLE IF NOT EXISTS car_data (
        id              SERIAL PRIMARY KEY,
        date            TIMESTAMPTZ,
        session_key     INTEGER,
        meeting_key     INTEGER,
        driver_number   INTEGER,
        brake           INTEGER,
        drs             INTEGER,
        n_gear          INTEGER,
        rpm             INTEGER,
        speed           INTEGER,
        throttle        INTEGER,
        fetched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, driver_number, session_key)
    );

    -- Drivers championship standings
    CREATE TABLE IF NOT EXISTS championship_drivers (
        id                  SERIAL PRIMARY KEY,
        session_key         INTEGER,
        meeting_key         INTEGER,
        driver_number       INTEGER,
        points_current      NUMERIC,
        points_start        NUMERIC,
        position_current    INTEGER,
        position_start      INTEGER,
        fetched_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (session_key, driver_number)
    );

    -- Teams championship standings
    CREATE TABLE IF NOT EXISTS championship_teams (
        id                  SERIAL PRIMARY KEY,
        session_key         INTEGER,
        meeting_key         INTEGER,
        team_name           TEXT,
        points_current      NUMERIC,
        points_start        NUMERIC,
        position_current    INTEGER,
        position_start      INTEGER,
        fetched_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (session_key, team_name)
    );

    -- Driver information
    CREATE TABLE IF NOT EXISTS drivers (
        id                  SERIAL PRIMARY KEY,
        session_key         INTEGER,
        meeting_key         INTEGER,
        driver_number       INTEGER,
        broadcast_name      TEXT,
        country_code        TEXT,
        first_name          TEXT,
        full_name           TEXT,
        last_name           TEXT,
        name_acronym        TEXT,
        team_colour         TEXT,
        team_name           TEXT,
        headshot_url        TEXT,
        fetched_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (session_key, driver_number)
    );

    -- Gap intervals between cars
    CREATE TABLE IF NOT EXISTS intervals (
        id                  SERIAL PRIMARY KEY,
        date                TIMESTAMPTZ,
        session_key         INTEGER,
        meeting_key         INTEGER,
        driver_number       INTEGER,
        gap_to_leader       TEXT,
        interval            TEXT,
        fetched_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, driver_number, session_key)
    );

    -- Lap timing data
    CREATE TABLE IF NOT EXISTS laps (
        id                      SERIAL PRIMARY KEY,
        session_key             INTEGER,
        meeting_key             INTEGER,
        driver_number           INTEGER,
        lap_number              INTEGER,
        date_start              TIMESTAMPTZ,
        lap_duration            NUMERIC,
        duration_sector_1       NUMERIC,
        duration_sector_2       NUMERIC,
        duration_sector_3       NUMERIC,
        i1_speed                INTEGER,
        i2_speed                INTEGER,
        st_speed                INTEGER,
        is_pit_out_lap          BOOLEAN,
        segments_sector_1       JSONB,
        segments_sector_2       JSONB,
        segments_sector_3       JSONB,
        fetched_at              TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (session_key, driver_number, lap_number)
    );

    -- GPS-like car location (x, y, z)
    CREATE TABLE IF NOT EXISTS location (
        id              SERIAL PRIMARY KEY,
        date            TIMESTAMPTZ,
        session_key     INTEGER,
        meeting_key     INTEGER,
        driver_number   INTEGER,
        x               INTEGER,
        y               INTEGER,
        z               INTEGER,
        fetched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, driver_number, session_key)
    );

    -- Race weekend meetings
    CREATE TABLE IF NOT EXISTS meetings (
        id                  SERIAL PRIMARY KEY,
        meeting_key         INTEGER UNIQUE,
        circuit_key         INTEGER,
        circuit_short_name  TEXT,
        country_code        TEXT,
        country_key         INTEGER,
        country_name        TEXT,
        date_start          TIMESTAMPTZ,
        gmt_offset          TEXT,
        location            TEXT,
        meeting_name        TEXT,
        meeting_official_name TEXT,
        year                INTEGER,
        fetched_at          TIMESTAMPTZ DEFAULT NOW()
    );

    -- Overtake events
    CREATE TABLE IF NOT EXISTS overtakes (
        id                          SERIAL PRIMARY KEY,
        date                        TIMESTAMPTZ,
        session_key                 INTEGER,
        meeting_key                 INTEGER,
        overtaking_driver_number    INTEGER,
        overtaken_driver_number     INTEGER,
        position                    INTEGER,
        fetched_at                  TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, overtaking_driver_number, session_key)
    );

    -- Pit lane events
    CREATE TABLE IF NOT EXISTS pit (
        id              SERIAL PRIMARY KEY,
        date            TIMESTAMPTZ,
        session_key     INTEGER,
        meeting_key     INTEGER,
        driver_number   INTEGER,
        lap_number      INTEGER,
        pit_duration    NUMERIC,
        fetched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, driver_number, session_key)
    );

    -- Car position on track
    CREATE TABLE IF NOT EXISTS position (
        id              SERIAL PRIMARY KEY,
        date            TIMESTAMPTZ,
        session_key     INTEGER,
        meeting_key     INTEGER,
        driver_number   INTEGER,
        position        INTEGER,
        fetched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, driver_number, session_key)
    );

    -- Race control messages (flags, penalties, VSC, SC…)
    CREATE TABLE IF NOT EXISTS race_control (
        id              SERIAL PRIMARY KEY,
        date            TIMESTAMPTZ,
        session_key     INTEGER,
        meeting_key     INTEGER,
        driver_number   INTEGER,
        category        TEXT,
        flag            TEXT,
        lap_number      INTEGER,
        message         TEXT,
        scope           TEXT,
        sector          INTEGER,
        fetched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, session_key, message)
    );

    -- Session information
    CREATE TABLE IF NOT EXISTS sessions (
        id                  SERIAL PRIMARY KEY,
        session_key         INTEGER UNIQUE,
        meeting_key         INTEGER,
        circuit_key         INTEGER,
        circuit_short_name  TEXT,
        country_code        TEXT,
        country_key         INTEGER,
        country_name        TEXT,
        date_end            TIMESTAMPTZ,
        date_start          TIMESTAMPTZ,
        gmt_offset          TEXT,
        location            TEXT,
        session_name        TEXT,
        session_type        TEXT,
        year                INTEGER,
        fetched_at          TIMESTAMPTZ DEFAULT NOW()
    );

    -- Session results (final classification)
    CREATE TABLE IF NOT EXISTS session_result (
        id              SERIAL PRIMARY KEY,
        session_key     INTEGER,
        meeting_key     INTEGER,
        driver_number   INTEGER,
        date            TIMESTAMPTZ,
        gap_to_leader   TEXT,
        interval        TEXT,
        lap             INTEGER,
        points          NUMERIC,
        position        INTEGER,
        status          TEXT,
        fetched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (session_key, driver_number)
    );

    -- Starting grid positions
    CREATE TABLE IF NOT EXISTS starting_grid (
        id              SERIAL PRIMARY KEY,
        session_key     INTEGER,
        meeting_key     INTEGER,
        driver_number   INTEGER,
        lap_number      INTEGER,
        position        INTEGER,
        tyre_age_at_start INTEGER,
        compound        TEXT,
        fetched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (session_key, driver_number)
    );

    -- Stint information (tyre compounds, laps on tyre)
    CREATE TABLE IF NOT EXISTS stints (
        id                  SERIAL PRIMARY KEY,
        session_key         INTEGER,
        meeting_key         INTEGER,
        driver_number       INTEGER,
        stint_number        INTEGER,
        lap_start           INTEGER,
        lap_end             INTEGER,
        compound            TEXT,
        tyre_age_at_start   INTEGER,
        fetched_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (session_key, driver_number, stint_number)
    );

    -- Team radio audio clips
    CREATE TABLE IF NOT EXISTS team_radio (
        id              SERIAL PRIMARY KEY,
        date            TIMESTAMPTZ,
        session_key     INTEGER,
        meeting_key     INTEGER,
        driver_number   INTEGER,
        recording_url   TEXT,
        fetched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, driver_number, session_key)
    );

    -- Track weather conditions
    CREATE TABLE IF NOT EXISTS weather (
        id                  SERIAL PRIMARY KEY,
        date                TIMESTAMPTZ,
        session_key         INTEGER,
        meeting_key         INTEGER,
        air_temperature     NUMERIC,
        humidity            NUMERIC,
        pressure            NUMERIC,
        rainfall            BOOLEAN,
        track_temperature   NUMERIC,
        wind_direction      INTEGER,
        wind_speed          NUMERIC,
        fetched_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (date, session_key)
    );
    """

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("✅ All tables created / verified.")


# ── API helpers ───────────────────────────────────────────────────────────────

def fetch_endpoint(endpoint: str, params: dict = None) -> list:
    """Fetch a single OpenF1 endpoint. Returns a list of records."""
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.error(f"❌ Error fetching {endpoint}: {e}")
        return []


# ── Upsert functions (one per table) ─────────────────────────────────────────

def upsert_car_data(conn, records):
    if not records:
        return 0
    rows = [
        (r.get("date"), r.get("session_key"), r.get("meeting_key"),
         r.get("driver_number"), r.get("brake"), r.get("drs"),
         r.get("n_gear"), r.get("rpm"), r.get("speed"), r.get("throttle"))
        for r in records
    ]
    sql = """
        INSERT INTO car_data (date, session_key, meeting_key, driver_number,
            brake, drs, n_gear, rpm, speed, throttle)
        VALUES %s
        ON CONFLICT (date, driver_number, session_key) DO UPDATE SET
            brake=EXCLUDED.brake, drs=EXCLUDED.drs, n_gear=EXCLUDED.n_gear,
            rpm=EXCLUDED.rpm, speed=EXCLUDED.speed, throttle=EXCLUDED.throttle,
            fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_championship_drivers(conn, records):
    if not records:
        return 0
    rows = [(r.get("session_key"), r.get("meeting_key"), r.get("driver_number"),
             r.get("points_current"), r.get("points_start"),
             r.get("position_current"), r.get("position_start")) for r in records]
    sql = """
        INSERT INTO championship_drivers (session_key, meeting_key, driver_number,
            points_current, points_start, position_current, position_start)
        VALUES %s
        ON CONFLICT (session_key, driver_number) DO UPDATE SET
            points_current=EXCLUDED.points_current, points_start=EXCLUDED.points_start,
            position_current=EXCLUDED.position_current, position_start=EXCLUDED.position_start,
            fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_championship_teams(conn, records):
    if not records:
        return 0
    rows = [(r.get("session_key"), r.get("meeting_key"), r.get("team_name"),
             r.get("points_current"), r.get("points_start"),
             r.get("position_current"), r.get("position_start")) for r in records]
    sql = """
        INSERT INTO championship_teams (session_key, meeting_key, team_name,
            points_current, points_start, position_current, position_start)
        VALUES %s
        ON CONFLICT (session_key, team_name) DO UPDATE SET
            points_current=EXCLUDED.points_current, points_start=EXCLUDED.points_start,
            position_current=EXCLUDED.position_current, position_start=EXCLUDED.position_start,
            fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_drivers(conn, records):
    if not records:
        return 0
    rows = [(r.get("session_key"), r.get("meeting_key"), r.get("driver_number"),
             r.get("broadcast_name"), r.get("country_code"), r.get("first_name"),
             r.get("full_name"), r.get("last_name"), r.get("name_acronym"),
             r.get("team_colour"), r.get("team_name"), r.get("headshot_url")) for r in records]
    sql = """
        INSERT INTO drivers (session_key, meeting_key, driver_number, broadcast_name,
            country_code, first_name, full_name, last_name, name_acronym,
            team_colour, team_name, headshot_url)
        VALUES %s
        ON CONFLICT (session_key, driver_number) DO UPDATE SET
            full_name=EXCLUDED.full_name, team_name=EXCLUDED.team_name,
            team_colour=EXCLUDED.team_colour, fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_intervals(conn, records):
    if not records:
        return 0
    rows = [(r.get("date"), r.get("session_key"), r.get("meeting_key"),
             r.get("driver_number"), str(r.get("gap_to_leader", "")),
             str(r.get("interval", ""))) for r in records]
    sql = """
        INSERT INTO intervals (date, session_key, meeting_key, driver_number,
            gap_to_leader, interval)
        VALUES %s
        ON CONFLICT (date, driver_number, session_key) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_laps(conn, records):
    if not records:
        return 0
    rows = [(r.get("session_key"), r.get("meeting_key"), r.get("driver_number"),
             r.get("lap_number"), r.get("date_start"), r.get("lap_duration"),
             r.get("duration_sector_1"), r.get("duration_sector_2"), r.get("duration_sector_3"),
             r.get("i1_speed"), r.get("i2_speed"), r.get("st_speed"),
             r.get("is_pit_out_lap"),
             json.dumps(r.get("segments_sector_1")),
             json.dumps(r.get("segments_sector_2")),
             json.dumps(r.get("segments_sector_3"))) for r in records]
    sql = """
        INSERT INTO laps (session_key, meeting_key, driver_number, lap_number,
            date_start, lap_duration, duration_sector_1, duration_sector_2,
            duration_sector_3, i1_speed, i2_speed, st_speed, is_pit_out_lap,
            segments_sector_1, segments_sector_2, segments_sector_3)
        VALUES %s
        ON CONFLICT (session_key, driver_number, lap_number) DO UPDATE SET
            lap_duration=EXCLUDED.lap_duration, fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_location(conn, records):
    if not records:
        return 0
    rows = [(r.get("date"), r.get("session_key"), r.get("meeting_key"),
             r.get("driver_number"), r.get("x"), r.get("y"), r.get("z")) for r in records]
    sql = """
        INSERT INTO location (date, session_key, meeting_key, driver_number, x, y, z)
        VALUES %s
        ON CONFLICT (date, driver_number, session_key) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_meetings(conn, records):
    if not records:
        return 0
    rows = [(r.get("meeting_key"), r.get("circuit_key"), r.get("circuit_short_name"),
             r.get("country_code"), r.get("country_key"), r.get("country_name"),
             r.get("date_start"), r.get("gmt_offset"), r.get("location"),
             r.get("meeting_name"), r.get("meeting_official_name"), r.get("year")) for r in records]
    sql = """
        INSERT INTO meetings (meeting_key, circuit_key, circuit_short_name, country_code,
            country_key, country_name, date_start, gmt_offset, location,
            meeting_name, meeting_official_name, year)
        VALUES %s
        ON CONFLICT (meeting_key) DO UPDATE SET
            meeting_name=EXCLUDED.meeting_name, fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_overtakes(conn, records):
    if not records:
        return 0
    rows = [(r.get("date"), r.get("session_key"), r.get("meeting_key"),
             r.get("overtaking_driver_number"), r.get("overtaken_driver_number"),
             r.get("position")) for r in records]
    sql = """
        INSERT INTO overtakes (date, session_key, meeting_key,
            overtaking_driver_number, overtaken_driver_number, position)
        VALUES %s
        ON CONFLICT (date, overtaking_driver_number, session_key) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_pit(conn, records):
    if not records:
        return 0
    rows = [(r.get("date"), r.get("session_key"), r.get("meeting_key"),
             r.get("driver_number"), r.get("lap_number"), r.get("pit_duration")) for r in records]
    sql = """
        INSERT INTO pit (date, session_key, meeting_key, driver_number,
            lap_number, pit_duration)
        VALUES %s
        ON CONFLICT (date, driver_number, session_key) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_position(conn, records):
    if not records:
        return 0
    rows = [(r.get("date"), r.get("session_key"), r.get("meeting_key"),
             r.get("driver_number"), r.get("position")) for r in records]
    sql = """
        INSERT INTO position (date, session_key, meeting_key, driver_number, position)
        VALUES %s
        ON CONFLICT (date, driver_number, session_key) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_race_control(conn, records):
    if not records:
        return 0
    rows = [(r.get("date"), r.get("session_key"), r.get("meeting_key"),
             r.get("driver_number"), r.get("category"), r.get("flag"),
             r.get("lap_number"), r.get("message"), r.get("scope"),
             r.get("sector")) for r in records]
    sql = """
        INSERT INTO race_control (date, session_key, meeting_key, driver_number,
            category, flag, lap_number, message, scope, sector)
        VALUES %s
        ON CONFLICT (date, session_key, message) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_sessions(conn, records):
    if not records:
        return 0
    rows = [(r.get("session_key"), r.get("meeting_key"), r.get("circuit_key"),
             r.get("circuit_short_name"), r.get("country_code"), r.get("country_key"),
             r.get("country_name"), r.get("date_end"), r.get("date_start"),
             r.get("gmt_offset"), r.get("location"), r.get("session_name"),
             r.get("session_type"), r.get("year")) for r in records]
    sql = """
        INSERT INTO sessions (session_key, meeting_key, circuit_key, circuit_short_name,
            country_code, country_key, country_name, date_end, date_start,
            gmt_offset, location, session_name, session_type, year)
        VALUES %s
        ON CONFLICT (session_key) DO UPDATE SET
            date_end=EXCLUDED.date_end, fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_session_result(conn, records):
    if not records:
        return 0
    rows = [(r.get("session_key"), r.get("meeting_key"), r.get("driver_number"),
             r.get("date"), str(r.get("gap_to_leader", "")),
             str(r.get("interval", "")), r.get("lap"), r.get("points"),
             r.get("position"), r.get("status")) for r in records]
    sql = """
        INSERT INTO session_result (session_key, meeting_key, driver_number, date,
            gap_to_leader, interval, lap, points, position, status)
        VALUES %s
        ON CONFLICT (session_key, driver_number) DO UPDATE SET
            position=EXCLUDED.position, points=EXCLUDED.points,
            status=EXCLUDED.status, fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_starting_grid(conn, records):
    if not records:
        return 0
    rows = [(r.get("session_key"), r.get("meeting_key"), r.get("driver_number"),
             r.get("lap_number"), r.get("position"), r.get("tyre_age_at_start"),
             r.get("compound")) for r in records]
    sql = """
        INSERT INTO starting_grid (session_key, meeting_key, driver_number,
            lap_number, position, tyre_age_at_start, compound)
        VALUES %s
        ON CONFLICT (session_key, driver_number) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_stints(conn, records):
    if not records:
        return 0
    rows = [(r.get("session_key"), r.get("meeting_key"), r.get("driver_number"),
             r.get("stint_number"), r.get("lap_start"), r.get("lap_end"),
             r.get("compound"), r.get("tyre_age_at_start")) for r in records]
    sql = """
        INSERT INTO stints (session_key, meeting_key, driver_number, stint_number,
            lap_start, lap_end, compound, tyre_age_at_start)
        VALUES %s
        ON CONFLICT (session_key, driver_number, stint_number) DO UPDATE SET
            lap_end=EXCLUDED.lap_end, fetched_at=NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_team_radio(conn, records):
    if not records:
        return 0
    rows = [(r.get("date"), r.get("session_key"), r.get("meeting_key"),
             r.get("driver_number"), r.get("recording_url")) for r in records]
    sql = """
        INSERT INTO team_radio (date, session_key, meeting_key, driver_number, recording_url)
        VALUES %s
        ON CONFLICT (date, driver_number, session_key) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def upsert_weather(conn, records):
    if not records:
        return 0
    rows = [(r.get("date"), r.get("session_key"), r.get("meeting_key"),
         r.get("air_temperature"), r.get("humidity"), r.get("pressure"),
         bool(r.get("rainfall")), r.get("track_temperature"),
         r.get("wind_direction"), r.get("wind_speed")) for r in records]
    sql = """
        INSERT INTO weather (date, session_key, meeting_key, air_temperature,
            humidity, pressure, rainfall, track_temperature, wind_direction, wind_speed)
        VALUES %s
        ON CONFLICT (date, session_key) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


# Map endpoint name → upsert function
UPSERT_MAP = {
    "car_data":               upsert_car_data,
    "championship_drivers":   upsert_championship_drivers,
    "championship_teams":     upsert_championship_teams,
    "drivers":                upsert_drivers,
    "intervals":              upsert_intervals,
    "laps":                   upsert_laps,
    "location":               upsert_location,
    "meetings":               upsert_meetings,
    "overtakes":              upsert_overtakes,
    "pit":                    upsert_pit,
    "position":               upsert_position,
    "race_control":           upsert_race_control,
    "sessions":               upsert_sessions,
    "session_result":         upsert_session_result,
    "starting_grid":          upsert_starting_grid,
    "stints":                 upsert_stints,
    "team_radio":             upsert_team_radio,
    "weather":                upsert_weather,
}


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(params: dict = None):
    """
    Main entry point. Fetches all configured endpoints and upserts into Postgres.

    params: optional query params applied to every endpoint call
            e.g. {"session_key": 9159} to fetch data for one session only.
    """
    log.info("🏎️  OpenF1 pipeline starting…")
    conn = get_connection()
    create_tables(conn)

    total_inserted = 0
    for endpoint in ENDPOINTS:
        log.info(f"  ↓ Fetching /{endpoint}…")
        records = fetch_endpoint(endpoint, params)
        upsert_fn = UPSERT_MAP.get(endpoint)
        if upsert_fn and records:
            n = upsert_fn(conn, records)
            log.info(f"    ✔  {n} record(s) upserted into '{endpoint}'")
            total_inserted += n
        else:
            log.info(f"    ⚠  No records returned for '{endpoint}'")
        time.sleep(REQUEST_DELAY)

    conn.close()
    log.info(f"🏁 Pipeline finished. Total records upserted: {total_inserted}")


if __name__ == "__main__":
    # Example: fetch a specific session.
    # Change or remove the params dict to fetch all available data.
    run_pipeline(params={"session_key": 9159})
