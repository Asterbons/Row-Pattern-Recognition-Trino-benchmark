"""Test which metrics collection methods work"""
import trino
import time
import requests

TRINO_CONFIG = {'host': 'localhost', 'port': 8080, 'user': 'admin', 'catalog': 'postgres', 'schema': 'public'}
conn = trino.dbapi.connect(**TRINO_CONFIG)
cur = conn.cursor()

# Run a more substantial test query (not just SELECT 1)
print("Running test query: SELECT count(*) FROM crime_data")
cur.execute('SELECT count(*) FROM crime_data')
result = cur.fetchone()
print(f"Result: {result[0]:,} rows")
test_id = cur._query.query_id
print(f"Query ID: {test_id}\n")

# Method 1: REST API
print("=" * 50)
print("Method 1: REST API")
print("=" * 50)
try:
    api_url = f"http://localhost:8080/v1/query/{test_id}"
    headers = {'X-Trino-User': 'admin'}
    response = requests.get(api_url, headers=headers, timeout=10)
    if response.status_code == 200:
        data = response.json()
        stats = data.get('queryStats', {})
        cpu = stats.get('totalCpuTime', 'N/A')
        mem = stats.get('peakUserMemoryReservation', stats.get('peakTotalMemoryReservation', 'N/A'))
        elapsed = stats.get('elapsedTime', 'N/A')
        print(f"  State: {data.get('state', 'N/A')}")
        print(f"  CPU Time: {cpu}")
        print(f"  Memory: {mem}")
        print(f"  Elapsed: {elapsed}")
        if cpu != 'N/A' and mem != 'N/A':
            print("  ✓ REST API WORKS!")
        else:
            print("  ✗ REST API returned incomplete data")
    else:
        print(f"  ✗ Failed: HTTP {response.status_code}")
except Exception as e:
    print(f"  ✗ Failed: {e}")

# Method 2: Tasks aggregation
print()
print("=" * 50)
print("Method 2: system.runtime.tasks")
print("=" * 50)
try:
    time.sleep(0.3)
    cur.execute(f"""
        SELECT SUM(split_cpu_time_ms), SUM(processed_input_rows)
        FROM system.runtime.tasks WHERE query_id = '{test_id}'
    """)
    row = cur.fetchone()
    cpu_ms = row[0]
    input_rows = row[1]
    print(f"  CPU time: {cpu_ms} ms")
    print(f"  Input rows: {input_rows}")
    if cpu_ms and cpu_ms > 0:
        print("  ✓ Tasks aggregation WORKS!")
    else:
        print("  ✗ Tasks aggregation returned no data")
except Exception as e:
    print(f"  ✗ Failed: {e}")

# Method 3: Timestamps
print()
print("=" * 50)
print("Method 3: Timestamp calculation")
print("=" * 50)
try:
    cur.execute(f"""
        SELECT CAST(to_unixtime("end") AS DOUBLE) - CAST(to_unixtime(started) AS DOUBLE)
        FROM system.runtime.queries
        WHERE query_id = '{test_id}' AND "end" IS NOT NULL
    """)
    row = cur.fetchone()
    if row and row[0]:
        print(f"  Elapsed: {row[0]:.4f}s")
        print("  ✓ Timestamp method WORKS!")
    else:
        print("  ✗ Timestamp method returned no data")
except Exception as e:
    print(f"  ✗ Failed: {e}")

print()
print("=" * 50)
print("SUMMARY")
print("=" * 50)
