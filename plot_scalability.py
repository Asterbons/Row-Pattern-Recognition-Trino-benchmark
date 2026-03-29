#!/usr/bin/env python3
# plot_scalability.py
# Reads output/scalability/n_*/mr6_results.csv and generates log-log scalability charts.
#
# Usage: python plot_scalability.py

import os
import glob

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

INPUT_DIR = os.path.join('output', 'scalability')
OUTPUT_DIR = os.path.join('figures', 'scalability')

OOM_STATUS = 'OOM'
MARKER_OK = 'o'
MARKER_OOM = 'X'
OOM_COLOR = 'red'


def load_results():
    pattern = os.path.join(INPUT_DIR, 'n_*', 'mr6_results.csv')
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No results found matching: {pattern}\n"
            f"Run: python run_scalability.py"
        )
    frames = [pd.read_csv(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    print(f"Loaded {len(df)} rows from {len(files)} result file(s)")
    return df


def compute_medians(df):
    """Per (query_pattern, dataset_size): median runtime; flag if any run was OOM."""
    rows = []
    for (pattern, size), group in df.groupby(['query_pattern', 'dataset_size']):
        ok = group[group['status'] == 'SUCCESS']
        oom = (group['status'] == OOM_STATUS).any()
        median_runtime = ok['runtime_sec'].median() if not ok.empty else None
        median_throughput = ok['throughput_input_rows_per_sec'].median() if not ok.empty else None
        rows.append({
            'query_pattern': pattern,
            'dataset_size': int(size),
            'median_runtime_sec': median_runtime,
            'median_throughput': median_throughput,
            'has_oom': oom,
        })
    return pd.DataFrame(rows).sort_values(['query_pattern', 'dataset_size'])


def short_label(query_pattern):
    """Strip leading number and .sql suffix for plot legends."""
    name = os.path.splitext(query_pattern)[0]
    parts = name.split('_', 1)
    return parts[1].replace('_', ' ') if len(parts) > 1 else name


def plot_runtime(summary, output_dir):
    fig, ax = plt.subplots(figsize=(10, 6))
    patterns = summary['query_pattern'].unique()

    for pattern in sorted(patterns):
        sub = summary[summary['query_pattern'] == pattern].copy()
        label = short_label(pattern)
        color = None

        # Plot successful points as a line
        ok = sub[sub['median_runtime_sec'].notna()]
        if not ok.empty:
            line, = ax.loglog(
                ok['dataset_size'], ok['median_runtime_sec'],
                marker=MARKER_OK, label=label,
            )
            color = line.get_color()

        # Mark OOM points
        oom = sub[sub['has_oom']]
        if not oom.empty:
            # Use last known runtime as y-position if available, else use axis bottom
            for _, row in oom.iterrows():
                y = row['median_runtime_sec'] if pd.notna(row['median_runtime_sec']) else ax.get_ylim()[0]
                ax.scatter(row['dataset_size'], y or 1e-3,
                           marker=MARKER_OOM, color=OOM_COLOR, s=120, zorder=5,
                           label='OOM' if 'OOM' not in [t.get_text() for t in ax.get_legend_handles_labels()[1]] else '')

    ax.set_xlabel('Dataset size (rows)', fontsize=12)
    ax.set_ylabel('Median runtime (s)', fontsize=12)
    ax.set_title('Scalability: Runtime vs. Dataset Size', fontsize=14)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))
    ax.grid(True, which='both', linestyle='--', alpha=0.5)
    ax.legend(fontsize=8, loc='upper left')

    _save(fig, output_dir, 'scalability_runtime')


def plot_throughput(summary, output_dir):
    fig, ax = plt.subplots(figsize=(10, 6))

    for pattern in sorted(summary['query_pattern'].unique()):
        sub = summary[(summary['query_pattern'] == pattern) & summary['median_throughput'].notna()]
        if sub.empty:
            continue
        ax.loglog(
            sub['dataset_size'], sub['median_throughput'],
            marker=MARKER_OK, label=short_label(pattern),
        )

    ax.set_xlabel('Dataset size (rows)', fontsize=12)
    ax.set_ylabel('Median throughput (rows/s)', fontsize=12)
    ax.set_title('Scalability: Throughput vs. Dataset Size', fontsize=14)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    ax.grid(True, which='both', linestyle='--', alpha=0.5)
    ax.legend(fontsize=8, loc='upper left')

    _save(fig, output_dir, 'scalability_throughput')


def plot_oom_heatmap(summary, output_dir):
    """Show which (query, size) combinations hit OOM."""
    patterns = sorted(summary['query_pattern'].unique(), reverse=True)
    sizes = sorted(summary['dataset_size'].unique())

    matrix = np.zeros((len(patterns), len(sizes)))
    for i, pattern in enumerate(patterns):
        for j, size in enumerate(sizes):
            cell = summary[(summary['query_pattern'] == pattern) & (summary['dataset_size'] == size)]
            if not cell.empty and cell.iloc[0]['has_oom']:
                matrix[i, j] = 1.0

    if matrix.sum() == 0:
        return  # Nothing to plot

    fig, ax = plt.subplots(figsize=(max(6, len(sizes) * 1.2), max(4, len(patterns) * 0.6)))
    im = ax.imshow(matrix, cmap='RdYlGn_r', vmin=0, vmax=1, aspect='auto')
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels([f'{s:,}' for s in sizes], rotation=30, ha='right')
    ax.set_yticks(range(len(patterns)))
    ax.set_yticklabels([short_label(p) for p in patterns])
    ax.set_title('OOM Occurrences (red = OOM)', fontsize=13)
    plt.colorbar(im, ax=ax, ticks=[0, 1], label='OOM')
    plt.tight_layout()

    _save(fig, output_dir, 'scalability_oom_heatmap')


def _save(fig, output_dir, name):
    for ext in ('pdf', 'png'):
        path = os.path.join(output_dir, f'{name}.{ext}')
        fig.savefig(path, bbox_inches='tight', dpi=150)
        print(f"  Saved: {path}")
    plt.close(fig)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = load_results()
    summary = compute_medians(df)

    csv_path = os.path.join(OUTPUT_DIR, 'scalability_summary.csv')
    summary.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    print("\nGenerating plots...")
    plot_runtime(summary, OUTPUT_DIR)
    plot_throughput(summary, OUTPUT_DIR)
    plot_oom_heatmap(summary, OUTPUT_DIR)

    print("\n[OK] Done. Output in:", OUTPUT_DIR)


if __name__ == '__main__':
    main()
