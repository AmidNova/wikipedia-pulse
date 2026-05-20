"""
edits_consumer.py

Consumer Kafka : lit les éditions accumulées dans le topic 'wikipedia-edits'
et les écrit dans la couche raw du datalake.

Lecture  : topic Kafka 'wikipedia-edits'
Écriture : datalake/raw/wikimedia_stream/Edits/{YYYYMMDD}/edits.ndjson
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

DATALAKE_ROOT = Path("/opt/airflow/datalake")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = "wikipedia-edits"

# Nombre max d'éditions à consommer par run (évite de boucler à l'infini)
MAX_MESSAGES = 1000
# Temps max d'attente sans nouveau message avant d'arrêter (ms)
CONSUMER_TIMEOUT_MS = 10000


def consume_from_kafka(max_messages: int = MAX_MESSAGES) -> list:
    """Lit les messages du topic Kafka et retourne la liste des éditions."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",      # lit depuis le début du topic
        enable_auto_commit=True,
        group_id="wikipedia-pulse-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
    )

    print(f"Consumer connecté à Kafka ({KAFKA_BOOTSTRAP}), topic '{TOPIC}'")

    edits = []
    for message in consumer:
        edits.append(message.value)
        if len(edits) % 50 == 0:
            print(f"  → {len(edits)} éditions consommées...")
        if len(edits) >= max_messages:
            break

    consumer.close()
    print(f"  → Total : {len(edits)} éditions consommées depuis Kafka")
    return edits


def save_to_raw(edits: list, date: datetime) -> Path:
    """Stocke les éditions en NDJSON dans la couche raw."""
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
    """Point d'entrée Airflow — remplace l'ancienne version SSE directe."""
    now = datetime.now(timezone.utc)
    print(f"=== edits_stream_to_raw (Kafka) | {now.strftime('%Y-%m-%d %H:%M')} UTC ===")

    edits = consume_from_kafka(max_messages=MAX_MESSAGES)

    if edits:
        save_to_raw(edits, now)
    else:
        print("  ⚠ Aucune édition dans le topic Kafka")

    print("=== edits_stream_to_raw done ===")
