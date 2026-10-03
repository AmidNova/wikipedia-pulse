"""
edits_consumer.py

Consumer Kafka : vide le backlog du topic 'wikipedia-edits' et écrit chaque
édition dans la couche raw, dans le dossier du jour de l'ÉVÉNEMENT (UTC),
pas du jour de consommation.

Lecture  : topic Kafka 'wikipedia-edits'
Écriture : datalake/raw/wikimedia_stream/Edits/{YYYYMMDD}/edits.ndjson

Garanties :
  - lit tout ce qui est présent au lancement (end offsets), pas un plafond fixe
  - commit des offsets seulement après écriture (at-least-once ; les doublons
    éventuels sont supprimés par edits_formatter)
"""

import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

from lib.common import target_date

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = "wikipedia-edits"
GROUP_ID = "wikipedia-pulse-consumer"

POLL_TIMEOUT_MS = 5000
# Polls vides consécutifs tolérés alors que le backlog n'est pas vidé
MAX_EMPTY_POLLS = 6


def raw_file(day: str) -> Path:
    return DATALAKE_ROOT / "raw" / "wikimedia_stream" / "Edits" / day / "edits.ndjson"


def edit_day(edit: dict) -> str:
    """Jour UTC (YYYYMMDD) de l'édition, d'après son timestamp unix."""
    return datetime.fromtimestamp(edit["timestamp"], tz=timezone.utc).strftime("%Y%m%d")


def is_valid(edit) -> bool:
    """Un message sans timestamp numérique ne peut pas être rangé par jour."""
    return isinstance(edit, dict) and isinstance(edit.get("timestamp"), (int, float))


def deserialize(raw: bytes):
    """JSON → dict ; None si le message est illisible (filtré ensuite)."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def group_by_day(edits: list) -> dict:
    groups = {}
    for edit in edits:
        groups.setdefault(edit_day(edit), []).append(edit)
    return groups


def append_to_raw(edits: list, day: str) -> None:
    output_file = raw_file(day)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "a", encoding="utf-8") as f:
        for edit in edits:
            f.write(json.dumps(edit, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _behind(consumer, partitions, end_offsets) -> bool:
    return any(consumer.position(tp) < end_offsets[tp] for tp in partitions)


def drain_to_raw(consumer, partitions) -> dict:
    """Lit le topic jusqu'aux end offsets du lancement, écrit par jour, puis commit.

    Retourne le nombre d'éditions écrites par jour.
    """
    end_offsets = consumer.end_offsets(partitions)
    counts = Counter()
    empty_polls = 0
    skipped = 0

    while _behind(consumer, partitions, end_offsets):
        batch = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
        records = [record.value for records in batch.values() for record in records]
        if not records:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                raise TimeoutError("Backlog Kafka non vidé : aucun message reçu malgré le retard")
            continue
        empty_polls = 0
        valid = [r for r in records if is_valid(r)]
        skipped += len(records) - len(valid)
        for day, edits in group_by_day(valid).items():
            append_to_raw(edits, day)
            counts[day] += len(edits)

    if skipped:
        print(f"  ⚠ {skipped} messages invalides ignorés")
    consumer.commit()
    return dict(counts)


def ensure_target_day_present(context) -> Path:
    """Échoue si aucune édition n'existe pour le jour traité."""
    path = raw_file(target_date(context).strftime("%Y%m%d"))
    if not path.exists():
        raise FileNotFoundError(f"Aucune édition pour ce jour ({path}) — producer Kafka arrêté ?")
    return path


def edits_stream_to_raw(**kwargs):
    """Point d'entrée Airflow."""
    print(f"=== edits_stream_to_raw (Kafka) | {target_date(kwargs).strftime('%Y-%m-%d')} ===")

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=deserialize,
    )
    try:
        partition_ids = consumer.partitions_for_topic(TOPIC)
        if not partition_ids:
            raise RuntimeError(f"Topic '{TOPIC}' introuvable — producer Kafka jamais lancé ?")
        partitions = [TopicPartition(TOPIC, p) for p in partition_ids]
        consumer.assign(partitions)
        counts = drain_to_raw(consumer, partitions)
    finally:
        consumer.close()

    for day, n in sorted(counts.items()):
        print(f"  → {day} : {n} éditions écrites")

    path = ensure_target_day_present(kwargs)
    print(f"  → OK : {path}")
    print("=== edits_stream_to_raw done ===")
