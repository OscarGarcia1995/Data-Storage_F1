"""
Configuration — OpenF1 → PostgreSQL Pipeline
Edit DB_CONFIG with your PostgreSQL credentials before running.
"""

# ── PostgreSQL connection ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "openf1_db",    # will be created via setup.sh
    "user":     "openf1_user",  # will be created via setup.sh
    "password": "TU_CONTRASEÑA_SEGURA",  # change this to a secure password
}

# ── OpenF1 API ────────────────────────────────────────────────────────────────
API_BASE_URL = "https://api.openf1.org/v1"

# All available endpoints (18 total)
ENDPOINTS = [
    "meetings",               # race weekends — fetch first (no session dependency)
    "sessions",               # sessions within a meeting
    "drivers",                # driver profiles per session
    "car_data",               # telemetry at ~3.7 Hz
    "championship_drivers",   # driver standings (race sessions only)
    "championship_teams",     # team standings (race sessions only)
    "intervals",              # gap-to-leader / interval
    "laps",                   # lap timing & sectors
    "location",               # car GPS position
    "overtakes",              # on-track overtakes
    "pit",                    # pit lane events
    "position",               # track position over time
    "race_control",           # flags, VSC, SC, penalties…
    "session_result",         # final classification
    "starting_grid",          # grid positions
    "stints",                 # tyre stints
    "team_radio",             # radio clips
    "weather",                # track & air conditions
]

# Seconds to wait between API calls (be polite to the free API 🙏)
REQUEST_DELAY = 0.5
