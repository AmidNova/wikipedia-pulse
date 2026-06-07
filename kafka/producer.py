"""
producer.py

Producer Kafka : écoute le flux SSE Wikimedia EventStreams en continu
et publie chaque édition humaine dans le topic Kafka 'wikipedia-edits'.

Ce script tourne en permanence (en dehors d'Airflow).
Lancement : python kafka/producer.py
"""

import json
import os
import signal
import sys

import requests
from kafka import KafkaProducer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "wikipedia-edits"
WATCHED_PROJECTS = {"en.wikipedia.org", "fr.wikipedia.org", "de.wikipedia.org", "es.wikipedia.org", "ru.wikipedia.org"}

# Compteur global pour le log
count = 0
running = True


def shutdown(signum, frame):
    """Arrêt propre sur Ctrl+C."""
    global running
    print(f"\nArrêt demandé. Total publié : {count} éditions.")
    running = False


def main():
    global count, running

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"Producer connecté à Kafka ({KAFKA_BOOTSTRAP})")
    print(f"Écoute du flux SSE Wikipedia → topic '{TOPIC}'")
    print("Ctrl+C pour arrêter.\n")

    headers = {"User-Agent": "wikipedia-pulse/1.0 (bigdata-project)"}

    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=None) as r:
        r.raise_for_status()

        for line in r.iter_lines():
            if not running:
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

            # Filtres : EN/FR Wikipedia, pas de bots, articles seulement
            if event.get("server_name") not in WATCHED_PROJECTS:
                continue
            if event.get("bot", False):
                continue
            if event.get("namespace") != 0:
                continue
            if event.get("type") not in {"edit", "new"}:
                continue

            edit = {
                "timestamp":  event.get("timestamp"),
                "project":    event.get("server_name"),
                "title":      event.get("title"),
                "user":       event.get("user"),
                "type":       event.get("type"),
                "minor":      event.get("minor", False),
                "length_old": event.get("length", {}).get("old", 0),
                "length_new": event.get("length", {}).get("new", 0),
                "comment":    event.get("comment", ""),
            }

            producer.send(TOPIC, value=edit)
            count += 1

            if count % 20 == 0:
                print(f"  → {count} éditions publiées dans Kafka")

    producer.flush()
    producer.close()
    print("Producer fermé proprement.")


if __name__ == "__main__":
    main()
