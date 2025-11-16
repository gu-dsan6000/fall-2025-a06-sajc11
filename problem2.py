#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name,
    col,
    regexp_extract,
    to_timestamp,
    min,
    max,
    count,
    lit,
)
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import max as spark_max

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd


def make_spark(master_url: str | None) -> SparkSession:
    """Create Spark session (cluster or local)."""
    builder = (
        SparkSession.builder
        .appName("Problem2_ClusterUsage")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "4g")
    )
    if master_url:
        builder = builder.master(master_url)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_fields(df):
    """
    Extract cluster_id, app_id, container_id from file_path.
    Then extract timestamps from log lines.
    """
    # 1) Add ID fields
    df_ids = (
        df
        .withColumn("cluster_id", regexp_extract(col("file_path"), r"application_(\d+)_", 1))
        .withColumn("application_id", regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1))
        .withColumn("app_number", regexp_extract(col("file_path"), r"application_\d+_(\d+)", 1))
    )

    # 2) Extract timestamp string
    df_ts = df_ids.withColumn(
        "timestamp_str",
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1),
    )

    # 3) Drop rows where we *didn't* match a timestamp (empty string)
    df_ts = df_ts.filter(col("timestamp_str") != "")

    # 4) Parse timestamp with a format string
    df_ts = df_ts.withColumn(
        "timestamp",
        to_timestamp("timestamp_str", "yy/MM/dd HH:mm:ss")
    )

    # 5) Drop helper column and keep only rows with valid timestamps
    return df_ts.drop("timestamp_str").filter(col("timestamp").isNotNull())


def write_df(df, path: Path):
    df.toPandas().to_csv(path, index=False)
    print(f"[OK] wrote: {path}")


def generate_visualizations(timeline_csv: Path, bar_png: Path, density_png: Path):
    """Use Pandas + Seaborn to create visualizations."""
    df = pd.read_csv(timeline_csv)

    # ---- Bar chart: Applications per cluster ----
    plt.figure(figsize=(10, 5))
    sns.countplot(x="cluster_id", data=df, palette="viridis")
    plt.xticks(rotation=45)
    plt.title("Number of Applications Per Cluster")
    plt.tight_layout()
    plt.savefig(bar_png)
    plt.close()
    print(f"[OK] wrote: {bar_png}")

    # ---- Density plot: durations of largest cluster ----
    df["duration_minutes"] = (
        pd.to_datetime(df["end_time"]) - pd.to_datetime(df["start_time"])
    ).dt.total_seconds() / 60.0

    largest_cluster = (
        df.groupby("cluster_id")["application_id"].count().sort_values(ascending=False).index[0]
    )
    df_largest = df[df["cluster_id"] == largest_cluster]

    plt.figure(figsize=(10, 5))
    sns.histplot(df_largest["duration_minutes"], kde=True, log_scale=True)
    plt.title(f"Duration Distribution (minutes) — Cluster {largest_cluster}")
    plt.xlabel("Duration (minutes, log scale)")
    plt.tight_layout()
    plt.savefig(density_png)
    plt.close()
    print(f"[OK] wrote: {density_png}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("master", nargs="?", default=None)
    ap.add_argument("--net-id", required=False, default="unknown")
    ap.add_argument("--skip-spark", action="store_true")
    args = ap.parse_args()

    outdir = Path("data/output")
    outdir.mkdir(parents=True, exist_ok=True)

    timeline_csv = outdir / "problem2_timeline.csv"
    cluster_summary_csv = outdir / "problem2_cluster_summary.csv"
    stats_txt = outdir / "problem2_stats.txt"
    bar_png = outdir / "problem2_bar_chart.png"
    density_png = outdir / "problem2_density_plot.png"

    if not args.skip_spark:
        spark = make_spark(args.master)

        print("[STEP] Reading logs from S3...")
        df = (
            spark.read.text(f"s3a://{args.net_id}-spark-logs/data/*/*")
            .withColumn("file_path", input_file_name())
        )

        print("[STEP] Extracting fields...")
        df2 = extract_fields(df)

        print("[STEP] Generating timeline...")
        timeline = (
            df2.groupBy("cluster_id", "application_id", "app_number")
            .agg(
                spark_min("timestamp").alias("start_time"),
                spark_max("timestamp").alias("end_time")
            )
            .orderBy("cluster_id", "app_number")
        )

        write_df(timeline, timeline_csv)

        print("[STEP] Building cluster summary...")
        cluster_summary = (
            timeline.groupBy("cluster_id")
            .agg(
                spark_min("start_time").alias("cluster_first_app"),
                spark_max("end_time").alias("cluster_last_app"),
                )
            .join(
                timeline.groupBy("cluster_id")
                .count()
                .withColumnRenamed("count", "num_applications"),
                on="cluster_id"
            )
        )

        write_df(cluster_summary, cluster_summary_csv)

        print("[STEP] Writing stats...")
        with open(stats_txt, "w") as f:
            num_clusters = cluster_summary.count()
            num_apps = timeline.count()
            f.write(f"Total unique clusters: {num_clusters}\n")
            f.write(f"Total applications: {num_apps}\n")
            f.write(f"Average applications per cluster: {num_apps / num_clusters:.2f}\n")

        print(f"[OK] wrote: {stats_txt}")

        spark.stop()

    # ---- Always generate plots (they use CSV files) ----
    print("[STEP] Generating visualizations...")
    generate_visualizations(timeline_csv, bar_png, density_png)

    print("[DONE] Problem 2 complete.")


if __name__ == "__main__":
    main()
