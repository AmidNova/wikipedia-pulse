"""
pageviews_formatter.py
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

DATALAKE_ROOT = Path("/opt/airflow/datalake")
PROJECTS = ["en_wikipedia", "fr_wikipedia"]


def convert_pageviews(project: str, date: datetime) -> Path:
    date_str = date.strftime("%Y%m%d")

    input_file = (
        DATALAKE_ROOT / "raw" / "wikimedia_analytics" / "Pageviews"
        / date_str / f"pageviews_{project}.json"
    )
    output_dir = (
        DATALAKE_ROOT / "formatted" / "wikimedia_analytics" / "Pageviews" / date_str
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"pageviews_{project}.snappy.parquet"

    print(f"Reading {input_file}...")

    with open(input_file, "r", encoding="utf-8") as f:
        raw = json.load(f)

    articles = raw["items"][0]["articles"]
    df = pd.DataFrame(articles)

    df["project"]   = project.replace("_", ".")
    df["date_utc"]  = pd.Timestamp(date.strftime("%Y-%m-%d"), tz="UTC").as_unit("us")
    df["article"]   = df["article"].str.replace("_", " ")
    df["views"]     = df["views"].astype(int)
    df["rank"]      = df["rank"].astype(int)

    df = df[["date_utc", "project", "rank", "article", "views"]]

    df.to_parquet(output_file, compression="snappy", index=False)
    print(f"  → {len(df)} articles saved to {output_file}")
    return output_file


def raw_to_formatted_pageviews(**kwargs):
    execution_date = kwargs["dag_run"].execution_date
    target_date = (execution_date - timedelta(days=1)).replace(tzinfo=timezone.utc)
    print(f"=== raw_to_formatted_pageviews | {target_date.strftime('%Y-%m-%d')} ===")
    for project in PROJECTS:
        try:
            convert_pageviews(project, target_date)
        except Exception as e:
            print(f"  ✗ Erreur pour {project}: {e}")
    print("=== raw_to_formatted_pageviews done ===")
