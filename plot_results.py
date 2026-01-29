import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json

# Configuration
INPUT_DIR = 'output'
INPUT_FILE = os.path.join(INPUT_DIR, 'mr6_results.csv')
METADATA_FILE = os.path.join(INPUT_DIR, 'mr6_metadata.json')
OUTPUT_DIR = 'figures'

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

def load_data():
    """Load benchmark results and metadata"""
    df = pd.read_csv(INPUT_FILE)
    
    # Filter only successful runs
    df_success = df[df['status'] == 'SUCCESS'].copy()
    
    # Load metadata if available
    metadata = {}
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
    
    return df_success, metadata

def plot_runtime_comparison(df):
    """Plot 1: Runtime comparison across query patterns"""
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Calculate median runtime per query
    medians = df.groupby('query_pattern')['runtime_sec'].median().sort_values()
    
    # Box plot with all iterations
    df_sorted = df.set_index('query_pattern').loc[medians.index].reset_index()
    sns.boxplot(data=df_sorted, x='query_pattern', y='runtime_sec', ax=ax)
    
    ax.set_xlabel('Query Pattern', fontsize=12)
    ax.set_ylabel('Runtime (seconds)', fontsize=12)
    ax.set_title('Runtime Comparison Across Query Patterns (Median + Quartiles)', fontsize=14)
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', rotation_mode='anchor')
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/runtime_comparison.pdf', dpi=300, bbox_inches='tight')
    plt.savefig(f'{OUTPUT_DIR}/runtime_comparison.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: runtime_comparison.pdf/.png")
    plt.close()

def plot_throughput_comparison(df):
    """Plot 2: Throughput comparison (INPUT rows processed per second)"""
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Use throughput_input_rows_per_sec instead
    medians = df.groupby('query_pattern')['throughput_input_rows_per_sec'].median().sort_values(ascending=False)
    df_sorted = df.set_index('query_pattern').loc[medians.index].reset_index()
    
    sns.barplot(data=df_sorted, x='query_pattern', y='throughput_input_rows_per_sec', 
                estimator='median', errorbar=('pi', 50), ax=ax)
    
    ax.set_xlabel('Query Pattern', fontsize=12)
    ax.set_ylabel('Throughput (input rows/second)', fontsize=12)
    ax.set_title('Throughput: Input Rows Processed per Second (Median with IQR)', fontsize=14)
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', rotation_mode='anchor')
    
    # Add formatting for large numbers
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/throughput_comparison.pdf', dpi=300, bbox_inches='tight')
    plt.savefig(f'{OUTPUT_DIR}/throughput_comparison.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: throughput_comparison.pdf/.png")
    plt.close()

def plot_cpu_memory(df):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    df_cpu = df[df['cpu_seconds'].notna() & (df['cpu_seconds'] != '')]
    df_mem = df[df['peak_memory_mb'].notna() & (df['peak_memory_mb'] != '')]
    
    if len(df_cpu) > 0:
        df_cpu['cpu_seconds'] = pd.to_numeric(df_cpu['cpu_seconds'])
        medians_cpu = df_cpu.groupby('query_pattern')['cpu_seconds'].median().sort_values()
        df_sorted_cpu = df_cpu.set_index('query_pattern').loc[medians_cpu.index].reset_index()
        
        sns.barplot(data=df_sorted_cpu, x='query_pattern', y='cpu_seconds', 
                    estimator='median', errorbar=('pi', 50), ax=ax1, color='steelblue')
        ax1.set_xlabel('Query Pattern', fontsize=12)
        ax1.set_ylabel('CPU Time (seconds, server-side)', fontsize=12)
        ax1.set_title('Server CPU Usage per Query Pattern', fontsize=14)
        
        plt.setp(ax1.get_xticklabels(), rotation=45, ha='right', rotation_mode='anchor')

    if len(df_mem) > 0:
        df_mem['peak_memory_mb'] = pd.to_numeric(df_mem['peak_memory_mb'])
        medians_mem = df_mem.groupby('query_pattern')['peak_memory_mb'].median().sort_values()
        df_sorted_mem = df_mem.set_index('query_pattern').loc[medians_mem.index].reset_index()
        
        sns.barplot(data=df_sorted_mem, x='query_pattern', y='peak_memory_mb', 
                    estimator='median', errorbar=('pi', 50), ax=ax2, color='coral')
        ax2.set_xlabel('Query Pattern', fontsize=12)
        ax2.set_ylabel('Peak Memory (MB, server-side)', fontsize=12)
        ax2.set_title('Server Memory Usage per Query Pattern', fontsize=14)
        
        plt.setp(ax2.get_xticklabels(), rotation=45, ha='right', rotation_mode='anchor')
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/cpu_memory_usage.png', dpi=300, bbox_inches='tight')
    plt.close()

def plot_runtime_vs_rows(df):
    """Plot 4: Runtime vs. Input Rows Processed (scalability analysis)"""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Aggregate by query pattern
    agg_df = df.groupby('query_pattern').agg({
        'runtime_sec': 'median',
        'input_rows_processed': 'median'
    }).reset_index()
    
    # Filter out rows with missing input data
    agg_df = agg_df[agg_df['input_rows_processed'].notna()]
    
    if len(agg_df) > 0:
        scatter = ax.scatter(agg_df['input_rows_processed'], agg_df['runtime_sec'], 
                            s=150, alpha=0.6, c=range(len(agg_df)), cmap='viridis')
        
        # Label points
        for idx, row in agg_df.iterrows():
            ax.annotate(row['query_pattern'], 
                       (row['input_rows_processed'], row['runtime_sec']),
                       fontsize=8, alpha=0.7, 
                       xytext=(5, 5), textcoords='offset points')
        
        ax.set_xlabel('Input Rows Processed (median)', fontsize=12)
        ax.set_ylabel('Runtime (seconds, median)', fontsize=12)
        ax.set_title('Scalability: Runtime vs. Input Data Size', fontsize=14)
        ax.grid(True, alpha=0.3)
        
        # Format x-axis for large numbers
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    else:
        ax.text(0.5, 0.5, 'No input row data available', 
                ha='center', va='center', transform=ax.transAxes, fontsize=12)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/runtime_vs_rows.pdf', dpi=300, bbox_inches='tight')
    plt.savefig(f'{OUTPUT_DIR}/runtime_vs_rows.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: runtime_vs_rows.pdf/.png")
    plt.close()

def plot_efficiency_heatmap(df):
    """Plot 5: Efficiency heatmap (runtime, CPU, memory normalized)"""
    # Filter and convert numeric columns
    df_clean = df.copy()
    
    # Handle CPU and memory columns that might have empty strings
    for col in ['cpu_seconds', 'peak_memory_mb']:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Calculate median values per query
    metrics = df_clean.groupby('query_pattern').agg({
        'runtime_sec': 'median',
        'cpu_seconds': 'median',
        'peak_memory_mb': 'median',
        'throughput_input_rows_per_sec': 'median'
    }).reset_index()
    
    # Create normalized columns (handle NaN values)
    def safe_normalize(series):
        max_val = series.max()
        if pd.isna(max_val) or max_val == 0:
            return pd.Series([0] * len(series), index=series.index)
        return series / max_val
    
    metrics['runtime_norm'] = safe_normalize(metrics['runtime_sec'])
    
    # For throughput, inverse normalization (higher is better, so we invert)
    throughput_max = metrics['throughput_input_rows_per_sec'].max()
    if pd.notna(throughput_max) and throughput_max > 0:
        metrics['throughput_norm'] = 1 - (metrics['throughput_input_rows_per_sec'] / throughput_max)
    else:
        metrics['throughput_norm'] = 0
    
    # Prepare heatmap data
    heatmap_cols = ['runtime_norm', 'throughput_norm']
    col_labels = ['Runtime', 'Throughput (inv)']
    
    # Add CPU and Memory only if we have data
    if metrics['cpu_seconds'].notna().any():
        metrics['cpu_norm'] = safe_normalize(metrics['cpu_seconds'])
        heatmap_cols.append('cpu_norm')
        col_labels.append('CPU Time')
    
    if metrics['peak_memory_mb'].notna().any():
        metrics['memory_norm'] = safe_normalize(metrics['peak_memory_mb'])
        heatmap_cols.append('memory_norm')
        col_labels.append('Memory')
    
    heatmap_data = metrics[['query_pattern'] + heatmap_cols]
    heatmap_data = heatmap_data.set_index('query_pattern')
    heatmap_data.columns = col_labels
    
    fig, ax = plt.subplots(figsize=(max(8, len(col_labels) * 2), 10))
    sns.heatmap(heatmap_data, annot=True, fmt='.2f', cmap='RdYlGn_r', 
                vmin=0, vmax=1, ax=ax, cbar_kws={'label': 'Normalized Cost (0=best, 1=worst)'})
    
    ax.set_title('Query Efficiency Heatmap (Normalized Metrics)', fontsize=14)
    ax.set_ylabel('Query Pattern', fontsize=12)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/efficiency_heatmap.pdf', dpi=300, bbox_inches='tight')
    plt.savefig(f'{OUTPUT_DIR}/efficiency_heatmap.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: efficiency_heatmap.pdf/.png")
    plt.close()

def generate_summary_stats(df, metadata):
    """Generate summary statistics table"""
    # Clean numeric columns
    df_clean = df.copy()
    for col in ['cpu_seconds', 'peak_memory_mb']:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    summary = df_clean.groupby('query_pattern').agg({
        'runtime_sec': ['median', 'mean', 'std', 'min', 'max'],
        'throughput_input_rows_per_sec': ['median', 'mean'],
        'cpu_seconds': ['median', 'mean'],
        'peak_memory_mb': ['median', 'max'],
        'input_rows_processed': 'median',
        'rows_returned': 'median'
    }).round(4)
    
    # Save to CSV
    summary.to_csv(f'{OUTPUT_DIR}/summary_statistics.csv')
    print("✓ Generated: summary_statistics.csv")
    
    # Create summary report
    with open(f'{OUTPUT_DIR}/summary_report.txt', 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("BENCHMARK SUMMARY REPORT - MR6 Requirements\n")
        f.write("=" * 70 + "\n\n")
        
        if metadata:
            f.write("System Information:\n")
            f.write(f"  System: {metadata.get('system', 'N/A')}\n")
            f.write(f"  Trino Version: {metadata.get('trino_version', 'N/A')}\n")
            
            trino_cfg = metadata.get('trino_config', {})
            f.write(f"  Cluster Nodes: {trino_cfg.get('cluster_nodes', 'N/A')}\n")
            
            bench_cfg = metadata.get('benchmark_config', {})
            f.write(f"  Warmup Runs: {bench_cfg.get('warmup_runs', 'N/A')}\n")
            f.write(f"  Measurement Iterations: {bench_cfg.get('iterations', 'N/A')}\n")
            f.write(f"  Timestamp: {metadata.get('timestamp', 'N/A')}\n\n")
        
        f.write("Benchmark Configuration:\n")
        f.write(f"  Total Query Patterns Tested: {df['query_pattern'].nunique()}\n")
        f.write(f"  Total Successful Runs: {len(df)}\n")
        f.write(f"  Failed Runs: {len(df[df['status'] != 'SUCCESS'])}\n\n")
        
        f.write("-" * 70 + "\n")
        f.write("PERFORMANCE RANKINGS\n")
        f.write("-" * 70 + "\n\n")
        
        f.write("Top 3 Fastest Queries (median runtime):\n")
        fastest = df_clean.groupby('query_pattern')['runtime_sec'].median().sort_values().head(3)
        for i, (query, time) in enumerate(fastest.items(), 1):
            f.write(f"  {i}. {query}: {time:.4f}s\n")
        
        f.write("\nTop 3 Slowest Queries (median runtime):\n")
        slowest = df_clean.groupby('query_pattern')['runtime_sec'].median().sort_values().tail(3)
        for i, (query, time) in enumerate(reversed(list(slowest.items())), 1):
            f.write(f"  {i}. {query}: {time:.4f}s\n")
        
        # Throughput rankings
        throughput_data = df_clean.groupby('query_pattern')['throughput_input_rows_per_sec'].median()
        throughput_data = throughput_data[throughput_data > 0].sort_values(ascending=False)
        
        if len(throughput_data) > 0:
            f.write("\nTop 3 Highest Throughput Queries (input rows/sec):\n")
            for i, (query, tput) in enumerate(list(throughput_data.head(3).items()), 1):
                f.write(f"  {i}. {query}: {tput:,.0f} rows/s\n")
        
        # Server metrics summary (if available)
        cpu_data = df_clean['cpu_seconds'].dropna()
        mem_data = df_clean['peak_memory_mb'].dropna()
        
        if len(cpu_data) > 0 or len(mem_data) > 0:
            f.write("\n" + "-" * 70 + "\n")
            f.write("SERVER-SIDE RESOURCE USAGE\n")
            f.write("-" * 70 + "\n")
            
            if len(cpu_data) > 0:
                f.write(f"\nCPU Time (server):\n")
                f.write(f"  Overall Median: {cpu_data.median():.2f}s\n")
                f.write(f"  Range: {cpu_data.min():.2f}s - {cpu_data.max():.2f}s\n")
            
            if len(mem_data) > 0:
                f.write(f"\nPeak Memory (server):\n")
                f.write(f"  Overall Median: {mem_data.median():.1f}MB\n")
                f.write(f"  Range: {mem_data.min():.1f}MB - {mem_data.max():.1f}MB\n")
        else:
            f.write("\n" + "-" * 70 + "\n")
            f.write("Note: Server-side metrics (CPU, Memory) were not available.\n")
            f.write("This may require access to Trino's system.runtime.queries table.\n")
        
        f.write("\n" + "=" * 70 + "\n")
    
    print("✓ Generated: summary_report.txt")

def main():
    print("=== MR7: Generating Plots and Analysis ===\n")
    
    # Load data
    df, metadata = load_data()
    print(f"Loaded {len(df)} successful benchmark runs\n")
    
    # Generate all plots
    plot_runtime_comparison(df)
    plot_throughput_comparison(df)
    plot_cpu_memory(df)
    plot_runtime_vs_rows(df)
    plot_efficiency_heatmap(df)
    
    # Generate statistics
    generate_summary_stats(df, metadata)
    
    print(f"\n✓ All plots saved to '{OUTPUT_DIR}/' directory")
    print("✓ Analysis complete!")

if __name__ == "__main__":
    main()
