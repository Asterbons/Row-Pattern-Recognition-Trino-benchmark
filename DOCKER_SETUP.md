# Docker Setup - Deployment Guide

This guide explains how to run the Trino benchmark project on any machine using Docker.

## Prerequisites

### Linux / Cloud Servers
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io docker-compose-plugin

# Start Docker daemon
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to docker group (optional, avoids using sudo)
sudo usermod -aG docker $USER
# Log out and back in for group changes to take effect
```

### Windows / macOS
- Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Launch Docker Desktop and wait for it to start

## Quick Start

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd trino
```

### 2. Start Services
```bash
# Start PostgreSQL and Trino (first run takes ~1-2 minutes)
docker compose up -d

# Check service health status
docker compose ps
```

**Expected output:**
- `postgres-jedi` - Status: `Up ... (healthy)`
- `trino-jedi` - Status: `Up ... (healthy)`

### 3. Wait for Initialization
- **PostgreSQL:** ~5-10 seconds to load crime_data CSV
- **Trino:** ~30-40 seconds to fully initialize

### 4. Run Benchmark
```bash
docker compose run --rm benchmark python benchmark.py
```

**Results appear in:** `output/mr6_results.csv`, `output/mr6_metadata.json`, `output/mr6_stats.json`

## Verification Tests

### Check Database
```bash
# Should return row count (50 for tiny, 1,000,000 for large dataset)
docker exec postgres-jedi psql -U trino -d postgres -c "SELECT count(*) FROM crime_data;"
```

### Check Trino Connectivity
```bash
# Should return the same row count
docker exec trino-jedi trino --execute "SELECT count(*) FROM postgres.public.crime_data"
```

## Configuration

### Switching Datasets

The default setup uses the **large** dataset (1M rows). To use the **tiny** dataset (50 rows):

1. Edit `init-scripts/init-db.sql`
2. Change line 14:
   ```sql
   FROM '/project_data/datasets/tiny/crime_data.csv'  -- tiny dataset
   -- FROM '/project_data/datasets/large/crime_data.csv'  -- large dataset
   ```
3. Recreate database:
   ```bash
   docker compose down -v
   docker compose up -d
   ```

## Common Commands

```bash
# Stop all services
docker compose down

# Stop and remove database volume (fresh start)
docker compose down -v

# View service logs
docker compose logs trino
docker compose logs database

# Access PostgreSQL directly
docker exec -it postgres-jedi psql -U trino -d postgres

# Access Trino CLI
docker exec -it trino-jedi trino

# Rebuild benchmark image after code changes
docker compose build benchmark
```

## Troubleshooting

### Services Won't Start
```bash
# Check Docker is running
docker --version

# View detailed logs
docker compose logs
```

### Database Not Initialized
```bash
# Check init script executed
docker compose logs database | grep "init-db.sql"

# Recreate from scratch
docker compose down -v
docker compose up -d
```

### Permission Errors (Linux)
```bash
# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker  # Refresh group membership
```

### "URL scheme http+docker" Error
You're using the old `docker-compose` (Python package). Use `docker compose` (space, not hyphen):
```bash
# Wrong (deprecated)
docker-compose up -d

# Correct (Docker Compose V2)
docker compose up -d
```

## Architecture

- **Database:** PostgreSQL 15 with auto-loaded crime_data table
- **Compute:** Trino (latest) querying PostgreSQL via catalog
- **Benchmark:** Python 3.11 container with volume-mounted code
- **Results:** Written to host machine's `output/` folder via volume mount

## Notes

- **No rebuilds needed:** Code changes are reflected immediately (volume mount strategy)
- **Health checks:** Ensure services start in correct order (database → Trino → benchmark)
- **Profiles:** Benchmark uses `tools` profile - only runs via `docker compose run`, not `up`
- **Ports exposed:** Trino (8080), PostgreSQL (5432) for debugging
