"""
edits_fetcher.py
Consomme le flux SSE Wikimedia EventStreams et stocke en NDJSON.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import requests

DATALAKE_ROOT = Path("/opt/airflow/datalake")
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
WATCHED_PROJECTS = {"en.wikipedia.org", "fr.wikipedia.org"}
MAX_EDITS = 500


def consume_stream(max_edits: int = MAX_EDITS) -> list:
    headers = {"User-Agent": "wikipedia-pulse/1.0 (bigdata-project)"}
    edits = []

    print(f"Connecting to EventStreams (target: {max_edits} edits)...")

    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=120) as r:
        r.raise_for_status()
        for line in r.iter_lines():
            if len(edits) >= max_edits:
                break

            if not line:
                continue

            decoded = line.decode("utf-8")
            if not decoded.startswith("data:"):
                continue

            try:
                event = json.loads(decoded[5:].strip())
            except json.JSONDecodeError:
                continue

            if event.get("server_name") not in WATCHED_PROJECTS:
                continue
            if event.get("bot", False):
                continue
            if event.get("namespace") != 0:
                continue
            if event.get("type") not in {"edit", "new"}:
                continue

            edits.append({
                "timestamp":  event.get("timestamp"),
                "project":    event.get("server_name"),
                "title":      event.get("title"),
                "user":       event.get("user"),
                "type":       event.get("type"),
                "minor":      event.get("minor", False),
                "length_old": event.get("length", {}).get("old", 0),
                "length_new": event.get("length", {}).get("new", 0),
                "comment":    event.get("comment", ""),
            })

            if len(edits) % 50 == 0:
                print(f"  → {len(edits)} edits collected...")

    print(f"  → Total: {len(edits)} éditions collectées")
    return edits


def save_to_raw(edits: list, date: datetime) -> Path:
    date_str = date.strftime("%Y%m%d")
    output_dir = DATALAKE_ROOT / "raw" / "wikimedia_stream" / "Edits" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "edits.ndjson"

    with open(output_file, "a", encoding="utf-8") as f:
        for edit in edits:
            f.write(json.dumps(edit, ensure_ascii=False) + "\n")

    print(f"  → Saved {len(edits)} edits to {output_file}")
    return output_file


def edits_stream_to_raw(**kwargs):
    now = datetime.now(timezone.utc)
    print(f"=== edits_stream_to_raw | {now.strftime('%Y-%m-%d %H:%M')} UTC ===")
    edits = consume_stream(max_edits=MAX_EDITS)
    save_to_raw(edits, now)
    print("=== edits_stream_to_raw done ===")
