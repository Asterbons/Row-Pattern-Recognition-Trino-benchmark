#!/usr/bin/env python3
"""
JSON Format Converter for Multi-System Scalability Comparison

Converts Oracle/Trino format to unified scalability format
Usage: python convert_to_scalability.py
"""

import json
from pathlib import Path

# Input files (adjust paths as needed)
FLINK_JSON = Path(r"C:\Uni\RPR(Semesterprojekt)\Generator\results\results_flink.json")
ORACLE_JSON = Path(r"C:\Uni\RPR(Semesterprojekt)\Generator\results\VM_Oracle_2.json")
TRINO_JSON = Path(r"C:\Uni\RPR(Semesterprojekt)\Generator\results\VM_Trino.json")

# Output
OUTPUT_JSON = Path("scalability_combined_all_systems.json")

# Query name mapping
QUERY_MAP = {
    '01_overlap_default.sql': '01_overlap_default',
    '02_overlap.sql': '02_overlap',
    '03_greedy.sql': '03_greedy',
    '03_reluctant.sql': '03_reluctant',
    '04_define.sql': '04_define',
    '05_ONE ROW PER MATCH.sql': '05_ONE_ROW_PER_MATCH',
    '06_ALL ROW PER MATCH.sql': '06_ALL_ROWS_PER_MATCH',
    '07_SUBSET.sql': '07_SUBSET_workaround',
    '08_WITHIN.sql': '08_WITHIN',
    '09_Nullwerte.sql': '09_Nullwerte'
}

# Dataset size mapping
SIZE_MAP = {
    '1k': 1000,
    '10k': 10000,
    '100k': 100000,
    '1M': 1000000,
    '10M': 10000000
}

def convert_oracle_trino(json_data, system_name):
    """Convert Oracle/Trino format to scalability format"""
    
    query_scalability = {}
    
    # Get all queries from first dataset
    first_dataset = json_data.get('1k', {})
    queries = [q for q in first_dataset.keys() if q.endswith('.sql')]
    
    for query_filename in queries:
        query_key = QUERY_MAP.get(query_filename, query_filename.replace('.sql', ''))
        
        query_scalability[query_key] = {
            'dataset_sizes': [],
            'elapsed_median': [],
            'cpu_median': [],
            'memory_median': [],
            'throughput_median': []
        }
        
        # Collect data across all dataset sizes
        for size_key, size_value in SIZE_MAP.items():
            dataset = json_data.get(size_key, {})
            query_data = dataset.get(query_filename, {})
            
            if query_data and query_data.get('runtime_median') is not None:
                query_scalability[query_key]['dataset_sizes'].append(size_value)
                query_scalability[query_key]['elapsed_median'].append(query_data.get('runtime_median'))
                query_scalability[query_key]['cpu_median'].append(query_data.get('cpu_median'))
                query_scalability[query_key]['memory_median'].append(None)  # Not available
                query_scalability[query_key]['throughput_median'].append(query_data.get('throughput_rows_s'))
    
    return {
        'benchmark_type': 'scalability',
        'system': system_name,
        'timestamp': json_data.get('metadata', {}).get('timestamp', 'unknown'),
        'dataset_sizes': list(SIZE_MAP.values()),
        'completed_sizes': list(SIZE_MAP.values()),
        'query_scalability': query_scalability,
        'metadata': json_data.get('metadata', {})
    }

def convert_flink(json_data):
    """Convert Flink format if needed (might already be in correct format)"""
    
    # Check if already in scalability format
    if 'query_scalability' in json_data:
        return json_data
    
    # If it's the single-run format, convert it
    if 'results' in json_data:
        # This is a single-run result, not scalability
        # We'll need to build scalability data differently
        # For now, return as-is and handle in the visualizer
        return json_data
    
    return json_data

def main():
    print("="*60)
    print("Multi-System Scalability JSON Converter")
    print("="*60)
    
    combined = {
        'systems': [],
        'flink': None,
        'trino': None,
        'oracle': None
    }
    
    # Load and convert Flink
    if FLINK_JSON.exists():
        print(f"\n✓ Loading Flink: {FLINK_JSON}")
        with open(FLINK_JSON) as f:
            flink_data = json.load(f)
        combined['flink'] = convert_flink(flink_data)
        combined['systems'].append('Flink')
        print(f"  → Flink data converted")
    else:
        print(f"\n⚠ Flink not found: {FLINK_JSON}")
    
    # Load and convert Oracle
    if ORACLE_JSON.exists():
        print(f"\n✓ Loading Oracle: {ORACLE_JSON}")
        with open(ORACLE_JSON) as f:
            oracle_data = json.load(f)
        combined['oracle'] = convert_oracle_trino(oracle_data, 'Oracle')
        combined['systems'].append('Oracle')
        
        # Count queries
        n_queries = len(combined['oracle']['query_scalability'])
        n_sizes = len(combined['oracle']['completed_sizes'])
        print(f"  → Oracle: {n_queries} queries × {n_sizes} sizes")
    else:
        print(f"\n⚠ Oracle not found: {ORACLE_JSON}")
    
    # Load and convert Trino
    if TRINO_JSON.exists():
        print(f"\n✓ Loading Trino: {TRINO_JSON}")
        with open(TRINO_JSON) as f:
            trino_data = json.load(f)
        combined['trino'] = convert_oracle_trino(trino_data, 'Trino')
        combined['systems'].append('Trino')
        
        # Count queries
        n_queries = len(combined['trino']['query_scalability'])
        n_sizes = len(combined['trino']['completed_sizes'])
        print(f"  → Trino: {n_queries} queries × {n_sizes} sizes")
    else:
        print(f"\n⚠ Trino not found: {TRINO_JSON}")
    
    # Save combined JSON
    print(f"\n{'='*60}")
    print(f"Saving combined JSON: {OUTPUT_JSON}")
    print(f"{'='*60}")
    
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(combined, f, indent=2)
    
    print(f"\n✅ Done!")
    print(f"\nSystems included: {', '.join(combined['systems'])}")
    print(f"\nNext steps:")
    print(f"  1. Open scalability_multi_system.html")
    print(f"  2. Upload this file: {OUTPUT_JSON}")
    print(f"  3. Or upload individual system JSONs separately")
    print()

if __name__ == '__main__':
    main()
