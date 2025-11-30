# PostgreSQL to ClickHouse Incremental Loader

An incremental data synchronization tool that transfers data from PostgreSQL to ClickHouse using Apache Spark. 
This project implements a batch-based incremental loading mechanism that tracks changes using `created_at` and `updated_at` timestamps.

## Features

- **Incremental Data Sync**: Efficiently syncs only new and updated records from PostgreSQL to ClickHouse
- **Dual Stream Support**: Handles both newly created records and updated records separately
- **Full Backfill**: Automatically performs a full backfill when ClickHouse table is empty
- **Scheduled Execution**: Supports cron-based scheduling for automated periodic syncs
- **Dockerized Infrastructure**: Complete Docker Compose setup for PostgreSQL and ClickHouse
- **Spark Integration**: Uses PySpark for distributed data processing
- **Idempotent Operations**: Safe to run multiple times without data duplication

## Prerequisites

- **Python 3.12**
- **Java 17**
- **Docker & Docker Compose** (for running databases)
- **Apache Spark 4.0.1+** (can be installed via `scripts/init_spark.sh`)
- **UV package manager** (recommended can be installed via `scripts/init_uv.sh`) or pip

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd pg_ch_incremental_loader
```

### 2. Install Python Dependencies

Using UV (recommended):
```bash
uv sync
```

if you don't have `UV` installed you can bootstrap it by executing 
```bash
bash ./scripts/init_uv.sh
```

Or using pip:
```bash
pip install -r requirements.txt
```

### 3. Install Apache Spark (if not already installed)

Run the provided installation script:
```bash
chmod +x scripts/init_spark.sh
./scripts/init_spark.sh
```

This script will:
- Install Java OpenJDK 11
- Download and install Apache Spark 4.0.1
- Configure environment variables
- Install Python dependencies (findspark, pyspark)

After installation, restart your shell or run:
```bash
source /etc/profile.d/spark.sh
```

### 4. Download JDBC Drivers

Ensure you have the following JDBC drivers in `spark/jar/`:
- `postgresql.jar` - PostgreSQL JDBC driver
- `clickhouse-jdbc-0.9.4-all-dependencies.jar` - ClickHouse JDBC driver

### 5. Configure Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=your_postgres_db

# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_clickhouse_password
CLICKHOUSE_DB=default
```

## Running the Project

### Step 1: Start Database Services

Start PostgreSQL and ClickHouse using Docker Compose:

```bash
docker-compose up -d
```

This will:
- Start PostgreSQL on port 5432
- Start ClickHouse on ports 8123 (HTTP) and 9000 (native)
- Automatically execute DDL scripts to create tables
- Initialize the database schemas

Verify services are running:
```bash
docker-compose ps
```

### Step 2: Populate PostgreSQL (Optional)

If you need to populate PostgreSQL with sample data, you can use the populator script:

```bash
chmod +x scripts/populator.sh
./scripts/populator.sh -U your_postgres_user -d your_postgres_db
```

### Step 3: Run the Incremental Loader

**Option A: Using UV (Recommended)**
```bash
uv run app/loader.py
```

**Option B: Using Python directly**
```bash
python app/loader.py
```

**Option C: Using the main entry point (legacy)**
```bash
python main.py
```

### Step 4: Set Up Scheduled Execution (Optional)

To run the loader automatically on a schedule (e.g., every 30 minutes), use the cron job script:

```bash
chmod +x scripts/cron_job_def.sh
./scripts/cron_job_def.sh
```

This will add a cron job that runs the loader every 30 minutes. You can modify the schedule in `scripts/cron_job_def.sh`.

## Project Structure

```
pg_ch_incremental_loader/
├── app/                    # Main application code
│   ├── config.py           # Configuration management
│   ├── loader.py           # Main incremental loader logic
│   └── logger.py           # Logging configuration
├── scripts/                # Utility scripts
│   ├── cron_job_def.sh    # Cron job setup script
│   ├── init_clickhouse.sh # ClickHouse initialization
│   ├── init_spark.sh      # Spark installation script
│   ├── init_uv.sh         # UV installation script
│   └── populator.sh       # PostgreSQL data population
├── sql/                   # Database schema definitions
│   ├── ddl-ch.sql        # ClickHouse DDL
│   └── ddl-pg.sql        # PostgreSQL DDL
├── spark/                # Spark configuration and JARs
│   ├── jar/              # JDBC driver JARs
│   └── conf/             # Spark configuration files
├── utils/                # Utility scripts
│   └── generate_csv_synth_data.py  # Synthetic data generator
├── docker-compose.yml    # Docker services configuration
├── main.py               # Legacy entry point
├── pyproject.toml        # Python project configuration
└── requirements.txt      # Python dependencies
```

## Script Descriptions

### Application Scripts

#### `app/loader.py`
**Main incremental loader script** - This is the core application that performs the incremental data synchronization.

**What it does:**
- Creates a Spark session with PostgreSQL and ClickHouse JDBC drivers
- Queries ClickHouse to find the last sync timestamps (`updated_at` and `created_at`)
- Reads data from PostgreSQL table `app_user_visits_fact`
- Performs full backfill if ClickHouse table is empty
- Filters and syncs only new/updated records based on timestamps
- Writes data to ClickHouse in batches
- Handles both updated records (where `updated_at` > last sync) and newly created records (where `updated_at` is NULL and `created_at` > last sync)

**Usage:**
```bash
uv run app/loader.py
```

#### `app/config.py`
**Configuration management module** - Centralizes all environment variable loading and provides database connection URLs.

**What it does:**
- Loads environment variables from `.env` file
- Provides PostgreSQL and ClickHouse connection parameters
- Constructs JDBC URLs for database connections
- Manages JDBC driver JAR file paths

#### `app/logger.py`
**Logging configuration module** - Sets up structured logging for the application.

**What it does:**
- Configures Python logging with custom formatter
- Provides timestamped, structured log output
- Includes function to suppress verbose Spark logs (optional)

### Utility Scripts

#### `scripts/init_spark.sh`
**Apache Spark installation script** - One-click installer for Apache Spark on Ubuntu/Debian systems.

**What it does:**
- Updates package lists
- Installs Java OpenJDK 11
- Downloads Apache Spark 4.0.1 with Hadoop 3 support
- Extracts and installs Spark to `/opt/spark`
- Sets up environment variables (`JAVA_HOME`, `SPARK_HOME`, `PATH`)
- Installs Python dependencies (findspark, pyspark)

**Usage:**
```bash
chmod +x scripts/init_spark.sh
./scripts/init_spark.sh
```

#### `scripts/init-clickhouse.sh`
**ClickHouse initialization script** - Ensures ClickHouse is ready and executes DDL if needed.

**What it does:**
- Waits for ClickHouse to be ready
- Checks if tables already exist
- Executes DDL script if tables don't exist
- Prevents duplicate DDL execution on container restarts

**Note:** This script is automatically executed by Docker Compose during ClickHouse container startup.

#### `scripts/populator.sh`
**PostgreSQL data population script** - Imports CSV data into PostgreSQL tables.

**What it does:**
- Reads CSV files from `resources/` directory
- Imports data into PostgreSQL using `\copy` command
- Handles branches, cashiers, customers, and stores dimension tables

**Usage:**
```bash
./scripts/populator.sh -U postgres_user -d postgres_db
```

**Note:** Requires CSV files in `resources/` directory with appropriate format.

#### `scripts/cron_job_def.sh`
**Cron job setup script** - Automatically adds a cron job for scheduled execution.

**What it does:**
- Adds a cron job that runs the loader every 30 minutes
- Writes output to `cron.log` file
- Prevents duplicate cron job entries

**Usage:**
```bash
chmod +x scripts/cron_job_def.sh
./scripts/cron_job_def.sh
```

**Note:** Modify the cron schedule in the script to change execution frequency.

### Utility Scripts (utils/)

#### `utils/generate_csv_synth_data.py`
**Synthetic data generator** - Generates CSV files with synthetic data for testing.

**What it does:**
- Generates synthetic data for branches, cashiers, customers, and stores
- Outputs CSV files to `resources/` directory
- Uses existing IDs from CSV files to maintain referential integrity

**Usage:**
```bash
python utils/generate_csv_synth_data.py
```

**Note:** Requires existing CSV files in `resources/` directory with ID columns.

### Legacy Scripts

#### `main.py`
**Legacy entry point** - Simple test script for basic PostgreSQL connection and data reading.

**What it does:**
- Creates a Spark session
- Reads sample data from PostgreSQL `app_user_visits_fact` table
- Displays first 10 rows

**Note:** This is a legacy script. Use `app/loader.py` for production incremental loading.

## How It Works

### Incremental Sync Logic

1. **Check Last Sync Timestamp**: Queries ClickHouse to find the maximum `updated_at` and `created_at` timestamps
2. **Full Backfill**: If ClickHouse table is empty, loads all records from PostgreSQL
3. **Incremental Load**: If table has data:
   - **Updated Records**: Filters PostgreSQL records where `updated_at > last_updated_timestamp`
   - **New Records**: Filters PostgreSQL records where `updated_at IS NULL` and `created_at > last_created_timestamp`
4. **Batch Write**: Writes filtered records to ClickHouse in batches of 5000 records
5. **Idempotency**: Uses `ReplacingMergeTree` engine in ClickHouse with `updated_at` to handle duplicates

### Data Flow

```
PostgreSQL (Source)
    ↓
Spark Session (PySpark)
    ↓
JDBC Read (Filter by timestamps)
    ↓
Data Processing
    ↓
JDBC Write (Batch insert)
    ↓
ClickHouse (Destination)
```

## Database Schema

### PostgreSQL Tables

- `branches` - Branch information
- `cashiers` - Cashier information
- `app_users` - Customer/app user information
- `stores` - Store information
- `app_user_visits_fact` - Fact table with visit records (main sync target)

### ClickHouse Tables

- `app_user_visits_fact` - ReplacingMergeTree table with the equivalent schema as PostgreSQL's

## Troubleshooting

### Common Issues

1. **Spark not found**: Run the `scripts/init_spark.sh`

2. **JDBC driver errors**: Verify JAR files exist in `spark/jar/` directory

3. **Connection errors**: Check `.env` file has correct database credentials

4. **Database connection timeout**: Ensure containers are running and accessible
   ```bash
   docker-compose ps
   ```

5. **Permission denied on scripts**: Make scripts executable
   ```bash
   chmod +x scripts/*.sh
   ```

## Development

### Running Tests

Test scripts are available in the `tests/` directory:
- `test-pg.py` - Tests PostgreSQL connection
- `test-ch.py` - Tests ClickHouse connection
- `test-spark.py` - Tests Spark integration and workflow

### Logging

Logs are written to stdout with structured format:
```
YYYY-MM-DD HH:MM:SS | LEVEL | MODULE | MESSAGE
```

Cron job logs are written to `cron.log` in the project root.

## Future Enhancements

See `todo.md` for planned features:
- Streaming option with CDC
- XMIN column support for PostgreSQL
- Feature flags
- Retry and idempotency improvements
- Additional synthetic data generation
