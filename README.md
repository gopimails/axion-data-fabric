# OpenSky Aircraft Data Ingestion Pipeline

A minimal Python POC for continuously fetching public aircraft surveillance data from the OpenSky Network REST API and storing it as Parquet files for later analytics.

## Overview

This pipeline polls the OpenSky Network `/api/states/all` endpoint for aircraft states over Switzerland, normalizes the data into tabular format, and stores snapshots as timestamped Parquet files in a partitioned directory structure. The design is simple, readable, and structured to enable later analytics workflows without overengineering.

### Key Features

- **Single API endpoint** — Uses only `/api/states/all` (no dependency on multiple endpoints)
- **Geographic filtering** — Restricts data to Switzerland via bounding box
- **Scheduled ingestion** — Runs every 5 minutes (configurable, or manually via CLI)
- **Clean normalization** — Converts state vectors into explicit, named columns
- **Partitioned storage** — Parquet files organized by `year/month/day/hour/`
- **Idempotent** — Multiple runs in the same hour create separate files
- **Basic retry logic** — Exponential backoff for transient HTTP failures
- **Minimal dependencies** — Python 3.11+, requests, pandas, pyarrow, python-dotenv

### Scope

**In scope:**
- Fetch state vectors from `/api/states/all`
- Filter to Switzerland bounding box
- Normalize response into rows with explicit columns
- Store as partitioned Parquet files
- Log useful messages to console
- Optional Airflow DAG for scheduling

**Out of scope:**
- Multiple OpenSky endpoints
- Aircraft-specific business logic
- Dashboards or visualization
- Machine learning or anomaly detection
- Distributed systems (Spark, Kafka, etc.)

## Project Structure

```
.
├── src/
│   ├── __init__.py           # Package marker
│   ├── config.py             # Environment loading, bounding box, constants
│   ├── auth.py               # OpenSky token management
│   ├── client.py             # API calls with retry logic
│   ├── normalize.py          # State vector → tabular rows
│   ├── storage.py            # Parquet writing with partitioning
│   └── main.py               # Orchestration entry point
├── dags/
│   └── opensky_ingest_dag.py # Airflow DAG (optional scheduling)
├── scripts/
│   └── read_sample.py        # Helper to read and display collected data
├── data/
│   └── raw/                  # Output directory for Parquet files
├── requirements.txt          # Python dependencies
├── .env.example              # Template for credentials and config
├── .gitignore                # Ignore rules (data/, .env, etc.)
└── README.md                 # This file
```

## Setup

### Prerequisites

- Python 3.11 or newer
- pip package manager

### Dev Container with Airflow

This repository includes a VS Code devcontainer that starts two Compose services:

- `app` - the Python development container VS Code attaches to
- `airflow` - a local Airflow scheduler and web UI sidecar

Reopen the repository in the devcontainer and Airflow will start alongside the editor container. The DAG folder is mounted from `dags/`, so `opensky_ingest_dag` is available in the Airflow UI.

- Airflow UI: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

The sidecar writes pipeline output to `data/raw` through the shared workspace mount. If `.env` exists, the Python pipeline can still load OpenSky credentials from it; otherwise it uses the unauthenticated API defaults.

### Installation

1. **Clone the repository** (if not already done)

2. **Create a virtual environment** (recommended)

   ```bash
   python3.11 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

   Note: Airflow is optional. If you only plan to run manually, you can remove it from `requirements.txt` or install a minimal set:
   ```bash
   pip install requests pandas pyarrow python-dotenv
   ```

4. **Configure environment**

   ```bash
   cp .env.example .env
   ```

   Edit `.env` and add your OpenSky OAuth v2 credentials (if using authenticated API):

   ```env
   CLIENT_ID=your_client_id
   CLIENT_SECRET=your_client_secret
   STORAGE_PATH=data/raw
   ```

   Leave CLIENT_ID and CLIENT_SECRET blank to use the free unauthenticated API tier.

## Usage

### Manual Ingestion (Single Run)

Fetch a single snapshot of aircraft states and store as Parquet:

```bash
python -m src.main
```

Or:

```bash
python src/main.py
```

**Expected output:**

```
2024-04-20 14:23:45,123 - src.main - INFO - Starting OpenSky data ingestion cycle
2024-04-20 14:23:45,234 - src.client - INFO - Fetching states (attempt 1/3): https://opensky-network.org/api/states/all
2024-04-20 14:23:47,456 - src.client - INFO - Successfully fetched states: 142 aircraft
2024-04-20 14:23:47,567 - src.normalize - INFO - Normalized 142 aircraft states from API response
2024-04-20 14:23:47,890 - src.storage - INFO - Successfully wrote 142 rows to data/raw/year=2024/month=04/day=20/hour=14/snapshot_20240420_142345_a1b2c3d4.parquet
2024-04-20 14:23:47,891 - src.main - INFO - Ingestion complete: 142 rows written to data/raw/year=2024/month=04/day=20/hour=14/snapshot_20240420_142345_a1b2c3d4.parquet
2024-04-20 14:23:47,892 - src.main - INFO - ✓ Data ingestion successful: data/raw/year=2024/month=04/day=20/hour=14/snapshot_20240420_142345_a1b2c3d4.parquet
```

### Explore Collected Data

View a sample of all collected Parquet files:

```bash
python scripts/read_sample.py
```

With options:

```bash
python scripts/read_sample.py --path data/raw --limit 20
```

**Expected output:**

```
================================================================================
Dataset shape: 142 rows, 17 columns
================================================================================

Columns:
snapshot_time_utc      datetime64[ns, UTC]
ingest_time_utc        datetime64[ns, UTC]
icao24                 object
callsign               object
origin_country         object
time_position          int64
last_contact           int64
longitude              float64
latitude               float64
baro_altitude          float64
on_ground              bool
velocity               float64
true_track             float64
vertical_rate          float64
geo_altitude           float64
squawk                 object
spi                    bool
position_source        int64
...

Sample data (first 10 rows):
  snapshot_time_utc  ingest_time_utc  icao24    callsign origin_country  ...
0 2024-04-20 14:23:47+00:00  2024-04-20 14:23:47.123+00:00  4b17f2  SWISS123  Switzerland  ...
1 2024-04-20 14:23:47+00:00  2024-04-20 14:23:47.124+00:00  4b17f3  UA4567    United States  ...
...

================================================================================
```

### Repeated Ingestion

Each run creates a new Parquet file with a unique timestamp. Run manually multiple times:

```bash
# Run at 14:00
python src/main.py
# Parquet created: data/raw/year=2024/month=04/day=20/hour=14/snapshot_20240420_140000_...parquet

# Run at 14:05
python src/main.py
# Another Parquet created: data/raw/year=2024/month=04/day=20/hour=14/snapshot_20240420_140500_...parquet

# Check your data
python scripts/read_sample.py
```

### Output Directory Structure

Parquet files are organized by ingestion timestamp:

```
data/raw/
├── year=2024/
│   └── month=04/
│       └── day=20/
│           ├── hour=13/
│           │   └── snapshot_20240420_130005_a1b2c3d4.parquet
│           ├── hour=14/
│           │   ├── snapshot_20240420_140023_b2c3d4e5.parquet
│           │   └── snapshot_20240420_140527_c3d4e5f6.parquet
│           └── hour=15/
│               └── snapshot_20240420_150001_d4e5f6g7.parquet
└── year=2025/
    └── ...
```

This partitioning enables efficient downstream analytics:
- Filter by date/hour ranges without loading full dataset
- Distribute processing across partitions
- Easy incremental loading in analytics tools

## Stored Columns

Each Parquet file contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_time_utc` | timestamp | API response timestamp (unix) |
| `ingest_time_utc` | timestamp | When the row was normalized (now) |
| `icao24` | string | Aircraft ICAO address |
| `callsign` | string | Flight callsign/identifier |
| `origin_country` | string | Country of aircraft registration |
| `time_position` | int | Timestamp of last position update (unix) |
| `last_contact` | int | Timestamp of last communication (unix) |
| `longitude` | float | Longitude (degrees) |
| `latitude` | float | Latitude (degrees) |
| `baro_altitude` | float | Barometric altitude (meters) |
| `on_ground` | bool | Is aircraft on ground |
| `velocity` | float | Velocity (m/s) |
| `true_track` | float | True heading (degrees 0–360) |
| `vertical_rate` | float | Vertical rate (m/s, positive = climbing) |
| `geo_altitude` | float | Geometric/GNSS altitude (meters) |
| `squawk` | string | Squawk code |
| `spi` | bool | Special Position Identification |
| `position_source` | int | Source of position: 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM |

All timestamps are in UTC. Null values are preserved where data is unavailable.

## Optional: Airflow Scheduling

To run ingestion automatically every 5 minutes using Apache Airflow:

### Initialize Airflow

```bash
# Set Airflow home (optional)
export AIRFLOW_HOME=$(pwd)/airflow_home

# Initialize database
airflow db init

# Create an admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### Start Scheduler and Web UI

In one terminal, start the scheduler:

```bash
airflow scheduler
```

In another terminal, start the web UI:

```bash
airflow webui --port 8080
```

Visit `http://localhost:8080` and log in with your credentials.

### View and Trigger DAG

- Look for `opensky_ingest_dag` in the DAG list
- Click to view details
- Use the "Trigger DAG" button to run manually
- Monitor logs in the web UI

The DAG is configured to run every 5 minutes (`*/5 * * * *` cron schedule).

### Check Logs

```bash
airflow dags test opensky_ingest_dag 2024-04-20
```

## Configuration

### Environment Variables

All configuration is managed via `.env` file:

| Variable | Default | Description |
|----------|---------|-------------|
| `CLIENT_ID` | (blank) | OpenSky OAuth v2 Client ID (optional) |
| `CLIENT_SECRET` | (blank) | OpenSky OAuth v2 Client Secret (optional) |
| `STORAGE_PATH` | `data/raw` | Root directory for Parquet files |

**Authentication:** The pipeline uses OAuth v2 client credentials flow to obtain a Bearer token. If CLIENT_ID and CLIENT_SECRET are provided, it will attempt to authenticate. If they're blank, the pipeline falls back to the unauthenticated (free tier) API, which is suitable for development and testing.
| `STORAGE_PATH` | `data/raw` | Root directory for Parquet files |

### API and Retry Settings

Configured in `src/config.py`:

| Setting | Value | Description |
|---------|-------|-------------|
| `API_TIMEOUT_SECONDS` | 10 | HTTP request timeout |
| `MAX_RETRIES` | 3 | Max retry attempts on failure |
| `RETRY_BACKOFF_FACTOR` | 2.0 | Exponential backoff multiplier |

## Bounding Box

The pipeline fetches data for Switzerland using this bounding box:

```
lamin=45.8  (min latitude, south)
lomin=5.9   (min longitude, west)
lamax=47.9  (max latitude, north)
lomax=10.5  (max longitude, east)
```

To change the region, edit `src/config.py` and update the `SWITZERLAND_BBOX` dictionary.

## Error Handling

### Authentication Failures

If credentials are invalid, the pipeline logs an error and fails immediately (no retry):

```
ERROR - Authentication failed: 403 Client Error...
```

**Solution:** Verify CLIENT_ID and CLIENT_SECRET in `.env` are correct, or run unauthenticated (leave both blank).

### Network Timeouts

If the API does not respond within 10 seconds, the pipeline retries with exponential backoff (up to 3 times):

```
WARNING - Timeout on attempt 1: ...
WARNING - Waiting 1.0s before retry...
```

After 3 attempts, it fails with a clear error message.

### Empty Responses

If the API returns no aircraft states (rare), the pipeline logs a warning and stores an empty Parquet file or skips the write:

```
WARNING - No aircraft states in API response
```

## Development & Testing

### Run Unit Tests (Optional)

```bash
pip install pytest
pytest tests/
```

### Check Code Quality

```python
# Basic import test
python -c "from src.main import ingest_once; print('✓ Imports OK')"
```

### Manual Integration Test

1. Set up `.env` with test credentials (or leave blank for free tier)
2. Run: `python src/main.py`
3. Verify: `ls -la data/raw/year=2024/month=04/day=20/hour=*/`
4. Inspect: `python scripts/read_sample.py`

## Future Analytics

The data is structured to enable:

- **Traffic counts over time** — Group by snapshot time and count aircraft
- **Altitude/velocity distributions** — Histograms of baro_altitude, velocity
- **Trajectory reconstruction** — Group by icao24, order by snapshot time
- **Origin country analysis** — Distribution of flights by country
- **Speed/altitude profiles** — Conditional aggregations for climbing/descending aircraft

Example Pandas query:

```python
import pandas as pd

# Load all data
df = pd.concat([pd.read_parquet(f) for f in list(Path('data/raw').rglob('*.parquet'))])

# Aircraft count by hour
df.groupby(df['snapshot_time_utc'].dt.floor('H')).size()

# Average altitude by origin country
df.groupby('origin_country')['baro_altitude'].mean()

# Trajectories by aircraft
df.sort_values(['icao24', 'snapshot_time_utc']).groupby('icao24').agg({
    'latitude': 'first',
    'longitude': 'first',
    'baro_altitude': 'mean'
})
```

## Troubleshooting

### `ModuleNotFoundError: No module named 'src'`

**Cause:** Running from wrong directory or virtual environment not activated.

**Solution:**
```bash
cd /path/to/axion-data-fabric
source venv/bin/activate
python -m src.main
```

### `FileNotFoundError: data/raw directory does not exist`

**Cause:** Directory is created automatically on first run, but `.gitignore` prevents tracking.

**Solution:** Just run the ingestion; the directory will be created.

### `ConnectionError: Max retries exceeded`

**Cause:** Network issue or OpenSky API is down.

**Solution:** Check internet connection. Retry later.

### `No rows to write to Parquet`

**Cause:** No aircraft found in Switzerland at that time, or API returned empty response.

**Solution:** Normal behavior. Run again later or expand bounding box.

## API Reference

### OpenSky Network `/api/states/all`

**Endpoint:** `https://opensky-network.org/api/states/all`

**Query Parameters:**
- `lamin`, `lomin`, `lamax`, `lomax` — Bounding box (optional)
- Authentication via HTTP Basic Auth or Bearer token

**Response:**

```json
{
  "time": 1713604427,
  "states": [
    ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", "latitude", "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate", "geo_altitude", "squawk", "spi", "position_source"],
    ...
  ]
}
```

See [OpenSky API Docs](https://opensky-network.org/apidoc/rest.html) for details.

## License

This project is provided as a learning POC.

## Next Steps

After validating the basic pipeline:

1. **Add filtering** — Filter by aircraft category, altitude, speed
2. **Add aggregations** — Compute hourly traffic counts, average metrics
3. **Add dashboarding** — Use Plotly/Streamlit for quick visualizations
4. **Add ML** — Detect anomalies or predict landing times
5. **Scale to production** — Add database backend, distributed processing

But keep it simple for now. The current design supports all of these extensions without major refactoring.

---

**Questions?** Check the FAQ below or refer to inline code comments.

## FAQ

**Q: Can I use this in production?**
A: This is a POC. For production, add error recovery (dead-letter queue), monitoring, alerting, and database persistence.

**Q: How much data will I accumulate?**
A: ~200 KB per snapshot (typical ~100–200 aircraft). Running every 5 minutes = ~6 MB/day, ~200 MB/month locally.

**Q: What if I want to use a different region?**
A: Edit the bounding box in `src/config.py` or add a CLI argument to `src/main.py`.

**Q: Can I use this with Spark or cloud storage?**
A: Yes. The Parquet format is compatible with Spark, S3, GCS, etc. Replace `src/storage.py` to write to cloud buckets.

**Q: Do I need Airflow?**
A: No. Use a system scheduler (cron, Task Scheduler) or just run manually. Airflow is optional for convenience.
