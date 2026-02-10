import trino
import time
import csv
import statistics
import os
import platform
import requests
from datetime import datetime

TRINO_CONFIG = {
    'host': os.environ.get('TRINO_HOST', 'localhost'),
    'port': int(os.environ.get('TRINO_PORT', '8080')),
    'user': 'admin',
    'catalog': 'postgres',
    'schema': 'public'
}

QUERY_DIR = 'queries'
OUTPUT_DIR = 'output'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'mr6_results.csv')
METADATA_FILE = os.path.join(OUTPUT_DIR, 'mr6_metadata.json')
STATS_FILE = os.path.join(OUTPUT_DIR, 'mr6_stats.json')
ITERATIONS = 5
WARMUP_RUNS = 1

def get_system_metadata(conn):
    """Collect system and Trino metadata (MR6 requirement)"""
    cur = conn.cursor()
    
    # Get Trino version
    try:
        cur.execute("SELECT version()")
        trino_version = cur.fetchone()[0]
    except:
        trino_version = "unknown"
    
    # Get Trino configuration
    trino_config_info = {}
    try:
        cur.execute("SHOW SESSION")
        session_props = cur.fetchall()
        trino_config_info['session_properties'] = {prop[0]: prop[1] for prop in session_props[:10]}
    except:
        pass
    
    # Get cluster info
    try:
        cur.execute("SELECT node_id, http_uri, node_version, coordinator, state FROM system.runtime.nodes")
        nodes = cur.fetchall()
        trino_config_info['cluster_nodes'] = len(nodes)
        trino_config_info['coordinator_count'] = sum(1 for n in nodes if n[3])
    except:
        trino_config_info['cluster_nodes'] = 'unknown'
    
    # Get host hardware info
    cpu_model = "Unknown"
    cpu_cores = os.cpu_count()
    total_ram_mb = 0
    
    try:
        # Linux / Docker approach
        if platform.system() == "Linux":
            with open('/proc/cpuinfo', 'r') as f:
                for line in f:
                    if "model name" in line:
                        cpu_model = line.split(":")[1].strip()
                        break
            
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if "MemTotal" in line:
                        # MemTotal:        16393932 kB
                        kb = int(line.split(":")[1].strip().split()[0])
                        total_ram_mb = int(kb / 1024)
                        break
        else:
            # Windows/Mac fallback (less detailed for CPU model usually)
            cpu_model = platform.processor()
    except:
        pass

    metadata = {
        'timestamp': datetime.now().isoformat(),
        'system': 'trino',
        'trino_version': trino_version,
        'hostname': platform.node(),
        'cpu_model': cpu_model,
        'cpu_cores': cpu_cores,
        'total_ram_mb': total_ram_mb,
        'os_kernel': platform.release(),
        'trino_config': trino_config_info,
        'client_python_version': platform.python_version(),
        'client_os': platform.system(),
        'benchmark_config': {
            'iterations': ITERATIONS,
            'warmup_runs': WARMUP_RUNS,
            'connection': {k: v for k, v in TRINO_CONFIG.items() if k != 'password'}
        }
    }
    
    return metadata

def get_input_row_count(cursor):
    """Get total input row count from base table"""
    try:
        cursor.execute("SELECT count(*) FROM crime_data")
        total_rows = cursor.fetchone()[0]
        return total_rows
    except:
        return None

def parse_duration_to_seconds(duration_str):
    """Parse Trino duration string (e.g., '1.23s', '456.78ms', '100ns') to seconds"""
    if not duration_str:
        return 0
    
    duration_str = str(duration_str).lower().strip()
    
    # Order matters: check longer suffixes first
    if 'ns' in duration_str:
        return float(duration_str.replace('ns', '')) / 1_000_000_000.0
    elif duration_str.endswith('n'):  # Alternative nanosecond notation
        return float(duration_str.rstrip('n')) / 1_000_000_000.0
    elif 'ms' in duration_str:
        return float(duration_str.replace('ms', '')) / 1000.0
    elif 'us' in duration_str:  # Microseconds
        return float(duration_str.replace('us', '')) / 1_000_000.0
    elif 's' in duration_str:
        return float(duration_str.replace('s', ''))
    elif 'm' in duration_str:
        return float(duration_str.replace('m', '')) * 60.0
    else:
        # Try to parse as numeric (assume seconds)
        try:
            return float(duration_str)
        except:
            return 0

def parse_memory_to_mb(memory_str):
    """Parse Trino memory string (e.g., '256.5MB', '1.2GB') to MB"""
    if not memory_str:
        return 0
    
    memory_str = str(memory_str).upper()
    
    if 'GB' in memory_str:
        return float(memory_str.replace('GB', '')) * 1024
    elif 'MB' in memory_str:
        return float(memory_str.replace('MB', ''))
    elif 'KB' in memory_str:
        return float(memory_str.replace('KB', '')) / 1024
    elif 'B' in memory_str and 'KB' not in memory_str and 'MB' not in memory_str:
        return float(memory_str.replace('B', '')) / (1024 ** 2)
    else:
        try:
            return float(memory_str) / (1024 ** 2)  # Assume bytes
        except:
            return 0

def get_query_stats_from_api(query_id):
    """
    Get query statistics from Trino REST API.
    Returns CPU time, memory, elapsed time, and row counts.
    """
    if not query_id or query_id == 'unknown':
        return {}
    
    try:
        api_url = f"http://{TRINO_CONFIG['host']}:{TRINO_CONFIG['port']}/v1/query/{query_id}"
        
        headers = {
            'X-Trino-User': TRINO_CONFIG['user']
        }
        
        response = requests.get(api_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            stats = data.get('queryStats', {})
            
            # CPU Time - try multiple field names (varies by Trino version)
            cpu_time = (stats.get('totalCpuTime') or 
                       stats.get('cpuTime') or 
                       stats.get('totalCpuTimeMillis') or
                       '0ms')
            
            # Elapsed Time
            elapsed_time = (stats.get('elapsedTime') or 
                          stats.get('executionTime') or 
                          stats.get('elapsedTimeMillis') or
                          '0ms')
            
            # Peak Memory - try multiple field names
            peak_memory = (stats.get('peakMemoryReservation') or 
                          stats.get('peakUserMemoryReservation') or
                          stats.get('peakTotalMemoryReservation') or
                          stats.get('peakTaskUserMemory') or
                          stats.get('peakUserMemory') or 
                          stats.get('peakTotalMemory'))
            
            # Check outputStage if not found
            if not peak_memory and 'outputStage' in data:
                output_stage = data['outputStage']
                if 'stageStats' in output_stage:
                    stage_stats = output_stage['stageStats']
                    peak_memory = (stage_stats.get('peakUserMemoryReservation') or
                                 stage_stats.get('peakMemoryReservation'))
            
            peak_memory_mb = parse_memory_to_mb(peak_memory) if peak_memory else 0
            
            # Input Rows
            input_rows = (stats.get('physicalInputRows') or 
                         stats.get('processedInputRows') or
                         stats.get('rawInputRows') or
                         stats.get('inputRows') or
                         0)
            
            # Output Rows
            output_rows = stats.get('outputRows') or stats.get('completedRows') or 0
            
            return {
                'query_id': query_id,
                'state': data.get('state', 'UNKNOWN'),
                'cpu_time_str': cpu_time,
                'cpu_time_sec': parse_duration_to_seconds(cpu_time),
                'elapsed_time_str': elapsed_time,
                'elapsed_time_sec': parse_duration_to_seconds(elapsed_time),
                'peak_memory_str': peak_memory if peak_memory else None,
                'peak_memory_mb': peak_memory_mb,
                'physical_input_rows': input_rows,
                'output_rows': output_rows,
                'source': 'REST API'
            }
        else:
            if response.status_code == 401:
                print(f"      Warning: API requires authentication (401)")
            else:
                print(f"      Warning: API returned status {response.status_code}")
            return {}
            
    except requests.exceptions.RequestException as e:
        print(f"      Warning: API request failed: {e}")
        return {}
    except Exception as e:
        print(f"      Warning: Error parsing API response: {e}")
        return {}

def measure_query_execution(cursor, sql_query, input_rows):
    """Execute query and measure comprehensive metrics via multiple methods"""
    
    # Wall-clock time
    start_time = time.time()
    query_id = None
    
    try:
        cursor.execute(sql_query)
        
        # Extract query ID from cursor
        if hasattr(cursor, '_query') and hasattr(cursor._query, 'query_id'):
            query_id = cursor._query.query_id
        
        rows = cursor.fetchall()
        row_count = len(rows)
        status = "SUCCESS"
        
    except Exception as e:
        status = f"ERROR: {e}"
        row_count = 0
        rows = []
    
    end_time = time.time()
    client_duration = end_time - start_time
    
    # Get server-side statistics from REST API
    server_stats = get_query_stats_from_api(query_id)
    
    # Extract metrics from server stats
    if server_stats:
        cpu_seconds = server_stats.get('cpu_time_sec', 0)
        peak_memory_mb = server_stats.get('peak_memory_mb', 0)
        
        # For input rows: use server value if available and > 0, otherwise use table estimate
        server_input_rows = server_stats.get('physical_input_rows')
        if server_input_rows and server_input_rows > 0:
            processed_rows = server_input_rows
        else:
            # Fallback to table size estimate
            processed_rows = input_rows or 0
        
        server_elapsed = server_stats.get('elapsed_time_sec', 0)
        stats_source = server_stats.get('source', 'unknown')
        
        # Use server elapsed time if available, otherwise client time
        runtime = server_elapsed if server_elapsed > 0 else client_duration
        
        # Convert 0 to None for cleaner output
        if cpu_seconds == 0:
            cpu_seconds = None
        if peak_memory_mb == 0:
            peak_memory_mb = None
    else:
        cpu_seconds = None
        peak_memory_mb = None
        processed_rows = input_rows or 0
        runtime = client_duration
        stats_source = 'client-only'
    
    # Calculate throughput: INPUT rows per second
    if processed_rows and processed_rows > 0 and runtime > 0:
        throughput_input = processed_rows / runtime
    else:
        throughput_input = 0
    
    return {
        'runtime_sec': runtime,
        'client_runtime_sec': client_duration,
        'rows_returned': row_count,
        'input_rows_processed': processed_rows,
        'throughput_input_rows_per_sec': throughput_input,
        'cpu_seconds': cpu_seconds,
        'peak_memory_mb': peak_memory_mb,
        'status': status,
        'query_id': query_id or 'unavailable',
        'stats_source': stats_source,
        'server_stats': server_stats
    }

def run_benchmark():
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Connection to Trino
    conn = trino.dbapi.connect(**TRINO_CONFIG)
    cur = conn.cursor()

    # Get system metadata
    metadata = get_system_metadata(conn)
    print("=" * 70)
    print("TRINO BENCHMARK - MR6 Performance Metrics")
    print("=" * 70)
    print(f"System: {metadata.get('system')}")
    print(f"Trino version: {metadata.get('trino_version')}")
    print(f"Cluster nodes: {metadata['trino_config'].get('cluster_nodes', 'N/A')}")
    print(f"Warmup runs: {WARMUP_RUNS}")
    print(f"Measurement iterations: {ITERATIONS}")
    print()
    
    # Test metrics collection method
    print("=" * 70)
    print("Testing REST API Connection")
    print("=" * 70)
    
    try:
        cur.execute("SELECT count(*) FROM crime_data")
        cur.fetchone()
        test_id = cur._query.query_id if hasattr(cur, '_query') else None
        
        if test_id:
            print(f"Test query: SELECT count(*) FROM crime_data")
            print(f"Query ID: {test_id}\n")
            
            api_stats = get_query_stats_from_api(test_id)
            if api_stats and api_stats.get('state'):
                print(f"[OK] REST API works!")
                print(f"  State: {api_stats.get('state', 'N/A')}")
                print(f"  CPU: {api_stats.get('cpu_time_str', 'N/A')}")
                print(f"  Memory: {api_stats.get('peak_memory_str', 'N/A')}")
                print(f"  Elapsed: {api_stats.get('elapsed_time_str', 'N/A')}")
            else:
                print(f"[FAIL] REST API unavailable or returned no metrics")
        else:
            print(f"[WARN] Cannot extract query_id from cursor")
    except Exception as e:
        print(f"[FAIL] Metrics test failed: {e}")
    
    print()
    
    # Save metadata
    import json
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=2)

    # Get baseline input size
    total_table_rows = get_input_row_count(cur)
    print(f"Table 'crime_data' contains {total_table_rows:,} rows" if total_table_rows else "Could not determine table size")
    print()

    # Prepare CSV
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        
        writer.writerow([
            'system',
            'query_pattern',
            'iteration',
            'runtime_sec',
            'client_runtime_sec',
            'input_rows_processed',
            'rows_returned',
            'throughput_input_rows_per_sec',
            'cpu_seconds',
            'peak_memory_mb',
            'status',
            'query_id'
        ])

        # Get query files
        query_files = sorted([f for f in os.listdir(QUERY_DIR) if f.endswith('.sql')])
        all_stats = {}

        for q_file in query_files:
            print("=" * 70)
            print(f"Pattern: {q_file}")
            print("=" * 70)
            
            with open(os.path.join(QUERY_DIR, q_file), 'r') as qf:
                sql_query = qf.read().strip().rstrip(';')

            input_rows = total_table_rows
            
            # WARMUP RUNS
            if WARMUP_RUNS > 0:
                print(f"Warmup ({WARMUP_RUNS} run(s))...")
                for i in range(WARMUP_RUNS):
                    try:
                        cur.execute(sql_query)
                        _ = cur.fetchall()
                        print(f"  Warmup {i+1} completed")
                    except Exception as e:
                        print(f"  Warmup {i+1} failed: {e}")
                print()
            
            print(f"Measurement runs ({ITERATIONS} iterations):")
            
            runtimes = []
            throughputs = []
            cpu_times = []
            memories = []

            # MEASUREMENT ITERATIONS
            for i in range(ITERATIONS):
                metrics = measure_query_execution(cur, sql_query, input_rows)
                
                if metrics['status'] == "SUCCESS":
                    runtimes.append(metrics['runtime_sec'])
                    throughputs.append(metrics['throughput_input_rows_per_sec'])
                    
                    if metrics['cpu_seconds'] is not None and metrics['cpu_seconds'] > 0:
                        cpu_times.append(metrics['cpu_seconds'])
                    if metrics['peak_memory_mb'] is not None and metrics['peak_memory_mb'] > 0:
                        memories.append(metrics['peak_memory_mb'])
                    
                    # Display metrics
                    print(f"  Run {i+1}: {metrics['runtime_sec']:.4f}s", end="")
                    if metrics['throughput_input_rows_per_sec'] > 0:
                        print(f" | {metrics['throughput_input_rows_per_sec']:,.0f} rows/s", end="")
                    if metrics['cpu_seconds'] is not None and metrics['cpu_seconds'] > 0:
                        print(f" | CPU: {metrics['cpu_seconds']:.2f}s", end="")
                    if metrics['peak_memory_mb'] is not None and metrics['peak_memory_mb'] > 0:
                        print(f" | Mem: {metrics['peak_memory_mb']:.1f}MB", end="")
                    print()
                else:
                    print(f"  Run {i+1}: FAILED ({metrics['status']})")

                # Write to CSV
                writer.writerow([
                    'trino',
                    q_file,
                    i+1,
                    metrics['runtime_sec'],
                    metrics['client_runtime_sec'],
                    metrics['input_rows_processed'],
                    metrics['rows_returned'],
                    metrics['throughput_input_rows_per_sec'],
                    metrics['cpu_seconds'] if metrics['cpu_seconds'] is not None else '',
                    metrics['peak_memory_mb'] if metrics['peak_memory_mb'] is not None else '',
                    metrics['status'],
                    metrics['query_id']
                ])

            # Statistics
            print(f"\n{'-' * 70}")
            print("STATISTICS:")
            if runtimes:
                print(f"  Runtime:")
                print(f"    Median: {statistics.median(runtimes):.4f}s")
                if len(runtimes) >= 4:
                    quartiles = statistics.quantiles(runtimes, n=4)
                    print(f"    Q1-Q3:  {quartiles[0]:.4f}s - {quartiles[2]:.4f}s")
                print(f"    Range:  {min(runtimes):.4f}s - {max(runtimes):.4f}s")
                
            if throughputs and any(t > 0 for t in throughputs):
                print(f"  Throughput (input):")
                print(f"    Median: {statistics.median(throughputs):,.0f} rows/s")
                
            if cpu_times:
                print(f"  CPU Time (server):")
                print(f"    Median: {statistics.median(cpu_times):.2f}s")
            else:
                print(f"  CPU Time: ⚠ Not available")
                
            if memories:
                print(f"  Peak Memory (server):")
                print(f"    Median: {statistics.median(memories):.1f}MB")
            else:
                print(f"  Peak Memory: ⚠ Not available")
            
            all_stats[q_file] = {
                'runtimes': runtimes,
                'throughputs': throughputs,
                'cpu_times': cpu_times,
                'memories': memories
            }
            
            print()

    # Save statistics
    stats_summary = {}
    for q_file, stats in all_stats.items():
        stats_summary[q_file] = {
            'runtime_median': statistics.median(stats['runtimes']) if stats['runtimes'] else None,
            'throughput_median': statistics.median(stats['throughputs']) if stats['throughputs'] else None,
            'cpu_median': statistics.median(stats['cpu_times']) if stats['cpu_times'] else None,
            'memory_median': statistics.median(stats['memories']) if stats['memories'] else None,
        }
    
    with open(STATS_FILE, 'w') as f:
        json.dump(stats_summary, f, indent=2)

    print("=" * 70)
    print("[OK] Benchmark completed!")
    print(f"[OK] Results: {OUTPUT_FILE}")
    print(f"[OK] Metadata: {METADATA_FILE}")
    print(f"[OK] Statistics: {STATS_FILE}")
    print("=" * 70)

if __name__ == "__main__":
    run_benchmark()
