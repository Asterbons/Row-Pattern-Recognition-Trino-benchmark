# Crime Data Generator

This script generates synthetic crime data for Berlin districts. It creates CSV files containing crime records with timestamps, locations, and crime types.

## Dependencies

*   Python 3.x
*   pandas
*   numpy

Install dependencies:
```bash
pip install pandas numpy
```

## Usage

Run the script from the command line:

```bash
python Generator.py --type [tiny|large] [options]
```

### Examples

**1. Generate a tiny dataset for testing:**
```bash
python Generator.py --type tiny
```
*Generates 50 rows in `../datasets/tiny/crime_data.csv`*

**2. Generate a large dataset:**
```bash
python Generator.py --type large --scale 1
```
*Generates 1,000,000 rows in `../datasets/large/scale_1/crime_data.csv`*

**3. Generate multiple files with custom complexity:**
```bash
python Generator.py --type large --count 5 --complexity 0.8
```

**4. Generate data with custom crime weights:**
```bash
python Generator.py --type large --custom_weights "ASSAULT:0.5,ROBBERY:0.2"
```

## Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| `--type` | **Required.** Size of dataset: `tiny` or `large`. | - |
| `--scale` | Scale factor for large datasets (1 = 1M rows). | `1` |
| `--partitions` | Number of districts to split data across. | `1` (for large) |
| `--complexity` | Distribution complexity. `> 0.5` for uniform distribution, `< 0.5` for realistic weights. | `0.3` |
| `--seed` | Seed for random number generation. | `42` |
| `--count` | Number of separate files to generate. | `1` |
| `--custom_weights` | Custom probability weights (e.g., `"THEFT:0.5"`). Remaining probability is distributed among other types. | `None` |

## Output

Files are saved to:
*   `../datasets/tiny/` for tiny datasets.
*   `../datasets/large/scale_N/` for large datasets.
