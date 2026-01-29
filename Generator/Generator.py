# Generator.py
# Author: Vet Lyong
# Date: 2025-12-07
# Description: Generates crime data for Berlin

#usage: generator.py [-h] --type {tiny,large} [--scale SCALE] [--partitions PARTITIONS]
#[--complexity COMPLEXITY] [--seed SEED] [--count COUNT] [--custom_weights "TYPE:PROBABILITY"]

#options:
#  -h, --help               show this help message and exit
#  --type {tiny,large}      Type of data to generate
#  --scale SCALE            Scale of the data
#  --partitions PARTITIONS  Number of partitions(Districts)
#  --complexity COMPLEXITY  Complexity of the data(affects weights distribution, 0:uniform, 1:realistic)
#  --seed SEED              Seed for random number generator
#  --count COUNT            Number of files to generate
#  --custom_weights "TYPE:PROBABILITY" (e.g. "THEFT:0.5,ROBBERY:0.1")


import pandas as pd
import numpy as np
import argparse
import os
from datetime import datetime, timedelta

# List of crimes and their probabilities (weights)
CRIME_TYPES = ['THEFT', 'BATTERY', 'CRIMINAL DAMAGE', 'ASSAULT', 'ROBBERY', 'NARCOTICS', 'HOMICIDE']
CRIME_WEIGHTS = [0.35,    0.20,      0.15,              0.10,      0.10,      0.09,        0.01]
BERLIN_DISTRICTS = [
    "Mitte", "Friedrichshain-Kreuzberg", "Pankow", "Charlottenburg-Wilmersdorf",
    "Spandau", "Steglitz-Zehlendorf", "Tempelhof-Schöneberg", "Neukölln",
    "Treptow-Köpenick", "Marzahn-Hellersdorf", "Lichtenberg", "Reinickendorf"
]
DISTRICT_META = {
    "Mitte":                      (52.5177, 13.4024),
    "Friedrichshain-Kreuzberg":   (52.5015, 13.4338),
    "Pankow":                     (52.5701, 13.4079),
    "Charlottenburg-Wilmersdorf": (52.5028, 13.2809),
    "Spandau":                    (52.5332, 13.1664),
    "Steglitz-Zehlendorf":        (52.4343, 13.2625),
    "Tempelhof-Schöneberg":       (52.4630, 13.3768),
    "Neukölln":                   (52.4571, 13.4533),
    "Treptow-Köpenick":           (52.4504, 13.5786),
    "Marzahn-Hellersdorf":        (52.5401, 13.5750),
    "Lichtenberg":                (52.5140, 13.4930),
    "Reinickendorf":              (52.6053, 13.2982)
}

# List of names for iteration
BERLIN_DISTRICTS = list(DISTRICT_META.keys())

def generate_crime_data(args, current_weights=None):
    np.random.seed(args.seed)
    
    if current_weights is None:
        current_weights = CRIME_WEIGHTS

    
    total_rows = args.rows
    num_partitions = args.partitions
    rows_per_part = total_rows // num_partitions
    
    print(f"Generating CRIME data: {total_rows} rows, {num_partitions} districts...")

    all_data = []
    
    # Start date
    current_time = datetime(2025, 1, 1, 0, 0, 0)
    
    # Coordinates of Berlin center (Alexanderplatz)
    base_lat = 52.5200
    base_lon = 13.4050

    for p in range(num_partitions):
        # A. Determine district name
        base_name = BERLIN_DISTRICTS[p % len(BERLIN_DISTRICTS)]
        
        # If many partitions are needed, add suffix for uniqueness (Mitte_1, Mitte_2...)
        if num_partitions > len(BERLIN_DISTRICTS):
            district = f"{base_name}_{p+1}"
        else:
            district = base_name
            
        # B. Get REAL coordinates of the district center
        center_lat, center_lon = DISTRICT_META[base_name]
        
        # C. Generate points around this center
        lat_noise = np.random.normal(0, 0.015, size=rows_per_part)
        lon_noise = np.random.normal(0, 0.015, size=rows_per_part)
        
        lats = center_lat + lat_noise
        lons = center_lon + lon_noise
        
        # 1. Time (Datetime) - strictly in order
        # Crimes happen every N minutes
        time_steps = np.arange(rows_per_part)
        timestamps = [current_time + timedelta(minutes=int(i)) for i in time_steps]
        
        # 2. Crime Type (Primary Type)
        # complexity affects weight distribution
        if args.complexity > 0.5:
            # More uniform distribution
            types = np.random.choice(CRIME_TYPES, size=rows_per_part)
        else:
            # Standard distribution
            # Use provided weights or default
            types = np.random.choice(CRIME_TYPES, size=rows_per_part, p=current_weights)
            
        # 3. Coordinates (lat, lon)
        # Generate crime "spots". 
        # Noise for district coordinates.
        lat_offset = np.random.normal(0, 0.01, size=rows_per_part) + (p * 0.02) # District shift
        lon_offset = np.random.normal(0, 0.01, size=rows_per_part) + (p * 0.02)
        
        lats = base_lat + lat_offset
        lons = base_lon + lon_offset

        # 4. Generate unique IDs
        start_id = p * rows_per_part
        ids = np.arange(start_id, start_id + rows_per_part)

        df_part = pd.DataFrame({
            'id': ids,
            'district': district,
            'datetime': timestamps,
            'primary_type': types,
            'lat': np.round(lats, 6),
            'lon': np.round(lons, 6)
        })
        
        all_data.append(df_part)

    final_df = pd.concat(all_data)
    return final_df

def save_file(df, base_path, subfolder, filename='crime_data.csv'):
    out_dir = os.path.join(base_path, subfolder)
    os.makedirs(out_dir, exist_ok=True)
    file_path = os.path.join(out_dir, filename)
    df.to_csv(file_path, index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"Saved: {file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', choices=['tiny', 'large'], required=True)
    parser.add_argument('--scale', type=int, default=1)
    parser.add_argument('--partitions', type=int, default=1)
    parser.add_argument('--complexity', type=float, default=0.3)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--count', type=int, default=1, help='Number of files to generate')
    parser.add_argument('--custom_weights', type=str, help='Custom weights in format "TYPE:PROB,TYPE:PROB..."', default=None)

    args = parser.parse_args()

    # --- Weight Renormalization Logic ---
    final_weights = CRIME_WEIGHTS[:]
    if args.custom_weights:
        try:
            # 1. Parse custom weights
            custom_map = {}
            for item in args.custom_weights.split(','):
                parts = item.strip().split(':')
                if len(parts) != 2:
                    raise ValueError(f"Invalid format for item: {item}")
                c_type = parts[0].strip().upper()
                c_prob = float(parts[1])
                
                if c_type not in CRIME_TYPES:
                    raise ValueError(f"Unknown crime type: {c_type}")
                if not (0 <= c_prob <= 1):
                    raise ValueError(f"Probability must be between 0 and 1: {c_prob}")
                    
                custom_map[c_type] = c_prob

            # 2. Normalize and Prepare Logic
            total_custom = sum(custom_map.values())
            non_custom_indices = [i for i, c in enumerate(CRIME_TYPES) if c not in custom_map]

            if total_custom > 1.0:
                print(f"Warning: Sum of custom weights is {total_custom:.4f} (> 1.0). Normalizing custom weights to sum to 1.0. Unspecified types will be 0.")
                for k in custom_map:
                    custom_map[k] /= total_custom
                remaining_prob = 0.0
            
            elif not non_custom_indices and total_custom < 1.0:
                # User specified ALL types, but they don't sum to 1.0
                print(f"Warning: All types specified but sum is {total_custom:.4f} (< 1.0). Normalizing to 1.0.")
                if total_custom > 0:
                    for k in custom_map:
                        custom_map[k] /= total_custom
                remaining_prob = 0.0
            
            else:
                # Partial specification with sum <= 1.0
                remaining_prob = 1.0 - total_custom

            new_weights = [0.0] * len(CRIME_TYPES)
            
            # Fill custom weights
            for c_type, prob in custom_map.items():
                idx = CRIME_TYPES.index(c_type)
                new_weights[idx] = prob
                
            # Fill remaining weights
            if non_custom_indices:
                original_sum = sum(CRIME_WEIGHTS[i] for i in non_custom_indices)
                
                if original_sum > 0:
                    for i in non_custom_indices:
                        ratio = CRIME_WEIGHTS[i] / original_sum
                        new_weights[i] = ratio * remaining_prob
                else:
                    if remaining_prob > 0:
                        dist_prob = remaining_prob / len(non_custom_indices)
                        for i in non_custom_indices:
                            new_weights[i] = dist_prob
                        
            final_weights = new_weights
                
            print(f"Custom weights applied: {dict(zip(CRIME_TYPES, np.round(final_weights, 4)))}")

        except Exception as e:
            print(f"Error parsing custom weights: {e}")
            exit(1)


    base_rows = 1_000_000 # Base for scale=1
    
    if args.type == 'tiny':
        args.rows = 50 
        args.partitions = 2
        folder = 'tiny'
    else:
        args.rows = base_rows * args.scale
        if args.partitions == 1: args.partitions = 10 * args.scale
        folder = f"large/scale_{args.scale}"

    base_seed = args.seed
    for i in range(args.count):
        args.seed = base_seed + i
        if args.count > 1:
            print(f"--- Generating file {i+1}/{args.count} with seed {args.seed} ---")
        
        df = generate_crime_data(args, final_weights)
        
        filename = 'crime_data.csv'
        if args.count > 1:
            filename = f'crime_data_{i}.csv'
            
        save_file(df, '../datasets', folder, filename) 