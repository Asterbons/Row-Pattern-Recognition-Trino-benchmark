"""
This script verifies the distribution of crime types in the generated dataset.
It reads 'datasets/tiny/crime_data.csv', calculates the frequency of each crime type, 
and prints the distribution as percentages.

Useful for custom weights
"""
import pandas as pd
import os

try:
    file_path = 'datasets/tiny/crime_data.csv'
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        exit(1)

    df = pd.read_csv(file_path)
    print("Total rows:", len(df))
    print("\nCrime Type Distribution:")
    counts = df['primary_type'].value_counts(normalize=True)
    for crime_type, frequency in counts.items():
        print(f"{crime_type}: {int(frequency * 100)}%")
    
    assault_prob = counts.get('ASSAULT', 0)
    robbery_prob = counts.get('ROBBERY', 0)
    
    #print(f"\nASSAULT (Target 0.5): {assault_prob}")
    #print(f"ROBBERY (Target 0.0): {robbery_prob}")

except Exception as e:
    print(f"Error: {e}")
