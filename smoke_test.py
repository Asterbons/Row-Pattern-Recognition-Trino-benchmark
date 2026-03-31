"""
Smoke test: verify Trino can query the crime_data table via the PostgreSQL catalog.
Exits 0 on success, non-zero on failure.
"""
import sys
import trino

conn = trino.dbapi.connect(
    host="trino",
    port=8080,
    user="smoke",
    catalog="postgres",
    schema="public",
)

cur = conn.cursor()

# Basic connectivity
cur.execute("SELECT 1")
assert cur.fetchone()[0] == 1, "SELECT 1 failed"

# Table exists and has rows
cur.execute("SELECT count(*) FROM crime_data")
count = cur.fetchone()[0]
assert count > 0, f"crime_data is empty (got {count} rows)"

print(f"OK: crime_data has {count} rows")
sys.exit(0)
