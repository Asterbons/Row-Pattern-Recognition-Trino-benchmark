# generate_all_scales.py
# Generates crime data CSVs for all scalability benchmark sizes.
# Skips generation if the CSV already exists.
#
# Usage: python generate_all_scales.py [--force]

import subprocess
import os
import sys

SCALES = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATASETS_DIR = os.path.join(PROJECT_DIR, 'datasets')
GENERATOR_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Generator.py')

def main():
    force = '--force' in sys.argv

    print("=" * 60)
    print("Scalability Data Generator")
    print("=" * 60)
    print(f"Sizes: {[f'{s:,}' for s in SCALES]}")
    print(f"Output: {DATASETS_DIR}/scalability/n_<size>/crime_data.csv")
    print(f"Force regenerate: {force}")
    print()

    for size in SCALES:
        csv_path = os.path.join(DATASETS_DIR, 'scalability', f'n_{size}', 'crime_data.csv')

        if os.path.exists(csv_path) and not force:
            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f"[SKIP] n={size:>12,}  |  Already exists ({file_size_mb:.1f} MB)")
            continue

        print(f"[GEN]  n={size:>12,}  |  Generating...", end="", flush=True)

        cmd = [
            sys.executable,
            GENERATOR_SCRIPT,
            '--rows', str(size),
            '--seed', '42',
            '--complexity', '0.3',
            '--output_dir', DATASETS_DIR,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            if os.path.exists(csv_path):
                file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
                print(f"  Done ({file_size_mb:.1f} MB)")
            else:
                print(f"  Done (file not found at expected path!)")
        else:
            print(f"  FAILED!")
            print(f"  stderr: {result.stderr}")
            sys.exit(1)

    print()
    print("=" * 60)
    print("[OK] All datasets generated!")
    print("=" * 60)

    # Summary
    print("\nGenerated files:")
    for size in SCALES:
        csv_path = os.path.join(DATASETS_DIR, 'scalability', f'n_{size}', 'crime_data.csv')
        if os.path.exists(csv_path):
            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f"  {csv_path}  ({file_size_mb:.1f} MB)")

if __name__ == '__main__':
    main()
