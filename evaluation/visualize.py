"""
Visualization module for DHT benchmark results.

Generates comparison plots from benchmark CSV data.

Usage:
    python -m evaluation.visualize
    python -m evaluation.visualize --input results/benchmark_results.csv
    python -m evaluation.visualize --output results/plots/
"""

import argparse
import os
import sys

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


# =============================================================================
# Configuration
# =============================================================================

# Plot styling
CHORD_COLOR = "#3498db"  # Blue
PASTRY_COLOR = "#e74c3c"  # Red
FIGURE_DPI = 150
FIGURE_SIZE_SCALABILITY = (14, 10)
FIGURE_SIZE_COMPARISON = (12, 6)

# Operation labels for display
OPERATION_LABELS = {
    "build": "Build (Node Join)",
    "insert": "Insert",
    "lookup": "Lookup",
    "update": "Update",
    "delete": "Delete",
    "node_join": "Node Join",
    "node_leave": "Node Leave",
}


# =============================================================================
# Data Loading
# =============================================================================

def load_benchmark_data(filepath: str) -> pd.DataFrame:
    """Load benchmark results from CSV."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Benchmark results not found: {filepath}")

    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} rows from {filepath}")
    return df


# =============================================================================
# Plot 1: Scalability (Hops vs Network Size)
# =============================================================================

def plot_scalability(df: pd.DataFrame, output_dir: str) -> str:
    """
    Generate scalability plot: Mean Hops vs Network Size for each operation.

    Creates a grid of subplots, one per operation, showing how hop count
    scales with network size for both Chord and Pastry.
    """
    operations = df["operation"].unique()
    n_ops = len(operations)

    # Create subplot grid (2 rows for 7 operations)
    n_cols = 4
    n_rows = 2

    fig, axes = plt.subplots(n_rows, n_cols, figsize=FIGURE_SIZE_SCALABILITY)
    axes = axes.flatten()

    for idx, op in enumerate(operations):
        ax = axes[idx]
        op_data = df[df["operation"] == op]

        # Plot Chord
        chord_data = op_data[op_data["protocol"] == "Chord"].sort_values("network_size")
        ax.plot(
            chord_data["network_size"],
            chord_data["mean_hops"],
            marker="o",
            color=CHORD_COLOR,
            linewidth=2,
            markersize=8,
            label="Chord"
        )

        # Plot Pastry
        pastry_data = op_data[op_data["protocol"] == "Pastry"].sort_values("network_size")
        ax.plot(
            pastry_data["network_size"],
            pastry_data["mean_hops"],
            marker="s",
            color=PASTRY_COLOR,
            linewidth=2,
            markersize=8,
            label="Pastry"
        )

        # Styling
        ax.set_title(OPERATION_LABELS.get(op, op), fontsize=11, fontweight="bold")
        ax.set_xlabel("Network Size (nodes)")
        ax.set_ylabel("Mean Hops")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper left")

        # Set x-ticks to actual network sizes
        if len(chord_data) > 0:
            ax.set_xticks(chord_data["network_size"].tolist())

    # Hide unused subplots
    for idx in range(n_ops, len(axes)):
        axes[idx].set_visible(False)

    # Overall title
    fig.suptitle("DHT Scalability: Mean Hops vs Network Size", fontsize=14, fontweight="bold")
    plt.tight_layout()

    # Save
    output_path = os.path.join(output_dir, "scalability_plot.png")
    plt.savefig(output_path, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_path}")
    return output_path


# =============================================================================
# Plot 2: Protocol Comparison Bar Chart
# =============================================================================

def plot_comparison_bars(df: pd.DataFrame, output_dir: str, network_size: int = None) -> str:
    """
    Generate bar chart comparing Chord vs Pastry for all operations.

    Args:
        df: Benchmark data
        output_dir: Output directory
        network_size: Which network size to show (default: largest available)
    """
    # Use largest network size if not specified
    if network_size is None:
        network_size = df["network_size"].max()

    # Filter to selected network size
    data = df[df["network_size"] == network_size]

    if len(data) == 0:
        print(f"No data for network_size={network_size}")
        return None

    operations = data["operation"].unique()

    # Prepare data for plotting
    chord_means = []
    pastry_means = []
    labels = []

    for op in operations:
        op_data = data[data["operation"] == op]
        chord_row = op_data[op_data["protocol"] == "Chord"]
        pastry_row = op_data[op_data["protocol"] == "Pastry"]

        chord_mean = chord_row["mean_hops"].values[0] if len(chord_row) > 0 else 0
        pastry_mean = pastry_row["mean_hops"].values[0] if len(pastry_row) > 0 else 0

        chord_means.append(chord_mean)
        pastry_means.append(pastry_mean)
        labels.append(OPERATION_LABELS.get(op, op))

    # Create bar chart
    fig, ax = plt.subplots(figsize=FIGURE_SIZE_COMPARISON)

    x = range(len(labels))
    width = 0.35

    bars1 = ax.bar([i - width/2 for i in x], chord_means, width,
                   label="Chord", color=CHORD_COLOR, edgecolor="black")
    bars2 = ax.bar([i + width/2 for i in x], pastry_means, width,
                   label="Pastry", color=PASTRY_COLOR, edgecolor="black")

    # Add value labels on bars
    for bar in bars1:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)

    for bar in bars2:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)

    # Styling
    ax.set_xlabel("Operation", fontsize=11)
    ax.set_ylabel("Mean Hops", fontsize=11)
    ax.set_title(f"Chord vs Pastry: Mean Hops per Operation ({network_size} nodes)",
                 fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    # Save
    output_path = os.path.join(output_dir, "comparison_bar_chart.png")
    plt.savefig(output_path, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_path}")
    return output_path


# =============================================================================
# Plot 3: Summary Table
# =============================================================================

def plot_summary_table(df: pd.DataFrame, output_dir: str) -> str:
    """
    Generate a summary table image showing key statistics.
    """
    # Pivot data for table
    operations = ["insert", "lookup", "update", "delete", "node_join", "node_leave"]
    network_sizes = sorted(df["network_size"].unique())

    # Build table data
    table_data = []

    for op in operations:
        row = [OPERATION_LABELS.get(op, op)]
        for size in network_sizes:
            chord_val = df[(df["operation"] == op) &
                          (df["protocol"] == "Chord") &
                          (df["network_size"] == size)]["mean_hops"]
            pastry_val = df[(df["operation"] == op) &
                           (df["protocol"] == "Pastry") &
                           (df["network_size"] == size)]["mean_hops"]

            chord_str = f"{chord_val.values[0]:.1f}" if len(chord_val) > 0 else "-"
            pastry_str = f"{pastry_val.values[0]:.1f}" if len(pastry_val) > 0 else "-"

            row.append(f"C:{chord_str}\nP:{pastry_str}")

        table_data.append(row)

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.axis("off")

    # Column headers
    columns = ["Operation"] + [f"N={size}" for size in network_sizes]

    # Create table
    table = ax.table(
        cellText=table_data,
        colLabels=columns,
        cellLoc="center",
        loc="center",
        colColours=[CHORD_COLOR] + ["#f0f0f0"] * len(network_sizes)
    )

    # Style table
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.8)

    # Color header row
    for j in range(len(columns)):
        table[(0, j)].set_facecolor("#2c3e50")
        table[(0, j)].set_text_props(color="white", fontweight="bold")

    # Title
    plt.title("Summary: Mean Hops (C=Chord, P=Pastry)", fontsize=13, fontweight="bold", pad=20)

    # Legend
    chord_patch = mpatches.Patch(color=CHORD_COLOR, label="Chord (C)")
    pastry_patch = mpatches.Patch(color=PASTRY_COLOR, label="Pastry (P)")
    ax.legend(handles=[chord_patch, pastry_patch], loc="upper right",
              bbox_to_anchor=(1, 1.15), ncol=2)

    plt.tight_layout()

    # Save
    output_path = os.path.join(output_dir, "summary_table.png")
    plt.savefig(output_path, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_path}")
    return output_path


# =============================================================================
# Plot 4: CRUD Operations Focus
# =============================================================================

def plot_crud_comparison(df: pd.DataFrame, output_dir: str) -> str:
    """
    Generate focused comparison of CRUD operations (insert, lookup, update, delete).
    """
    crud_ops = ["insert", "lookup", "update", "delete"]
    crud_data = df[df["operation"].isin(crud_ops)]

    if len(crud_data) == 0:
        print("No CRUD operation data found")
        return None

    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    axes = axes.flatten()

    for idx, op in enumerate(crud_ops):
        ax = axes[idx]
        op_data = crud_data[crud_data["operation"] == op]

        # Plot both protocols
        for protocol, color, marker in [("Chord", CHORD_COLOR, "o"), ("Pastry", PASTRY_COLOR, "s")]:
            pdata = op_data[op_data["protocol"] == protocol].sort_values("network_size")

            ax.errorbar(
                pdata["network_size"],
                pdata["mean_hops"],
                yerr=pdata["std_hops"],
                marker=marker,
                color=color,
                linewidth=2,
                markersize=8,
                capsize=5,
                label=protocol
            )

        ax.set_title(OPERATION_LABELS.get(op, op), fontsize=12, fontweight="bold")
        ax.set_xlabel("Network Size (nodes)")
        ax.set_ylabel("Mean Hops (± std)")
        ax.grid(True, alpha=0.3)
        ax.legend()

        if len(pdata) > 0:
            ax.set_xticks(pdata["network_size"].tolist())

    fig.suptitle("CRUD Operations: Chord vs Pastry (with error bars)",
                 fontsize=14, fontweight="bold")
    plt.tight_layout()

    output_path = os.path.join(output_dir, "crud_comparison.png")
    plt.savefig(output_path, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_path}")
    return output_path


# =============================================================================
# Main
# =============================================================================

def generate_all_plots(input_file: str, output_dir: str) -> None:
    """Generate all visualization plots."""
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    df = load_benchmark_data(input_file)

    print(f"\nGenerating plots to: {output_dir}")
    print("-" * 50)

    # Generate each plot
    plot_scalability(df, output_dir)
    plot_comparison_bars(df, output_dir)
    plot_summary_table(df, output_dir)
    plot_crud_comparison(df, output_dir)

    print("-" * 50)
    print("All plots generated successfully!")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate DHT benchmark visualizations")
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=os.path.join(config.RESULTS_DIR, "benchmark_results.csv"),
        help="Input CSV file from benchmark"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=config.RESULTS_DIR,
        help="Output directory for plots"
    )
    args = parser.parse_args()

    try:
        generate_all_plots(args.input, args.output)
        return 0
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Run the benchmark first: python -m evaluation.benchmark")
        return 1
    except Exception as e:
        print(f"Error generating plots: {e}")
        raise


if __name__ == "__main__":
    sys.exit(main())
