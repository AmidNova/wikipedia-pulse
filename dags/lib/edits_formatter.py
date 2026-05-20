"""
edits_formatter.py
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DATALAKE_ROOT = Path("/opt/airflow/datalake")


def convert_edits(date: datetime) -> Path:
    date_str = date.strftime("%Y%m%d")

    input_file = DATALAKE_ROOT / "raw" / "wikimedia_stream" / "Edits" / date_str / "edits.ndjson"
    output_dir = DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "edits.snappy.parquet"

    print(f"Reading {input_file}...")

    df = pd.read_json(input_file, lines=True)

    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).astype("datetime64[us, UTC]")
    df["edit_size"]     = df["length_new"] - df["length_old"]
    df["title"]         = df["title"].str.strip()
    df["comment"]       = df["comment"].fillna("")

    df = df[[
        "timestamp_utc", "project", "title", "user",
        "type", "minor", "edit_size", "length_old", "length_new", "comment"
    ]]

    df.to_parquet(output_file, compression="snappy", index=False)
    print(f"  → {len(df)} edits saved to {output_file}")
    return output_file


def raw_to_formatted_edits(**kwargs):
    from datetime import timezone
    execution_date = kwargs["dag_run"].execution_date
    target_date = execution_date.replace(tzinfo=timezone.utc)
    print(f"=== raw_to_formatted_edits | {target_date.strftime('%Y-%m-%d')} ===")
    try:
        convert_edits(target_date)
    except Exception as e:
        print(f"  ✗ Erreur : {e}")
    print("=== raw_to_formatted_edits done ===")
