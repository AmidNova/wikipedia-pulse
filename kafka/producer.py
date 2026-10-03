"""
producer.py - Producer Kafka avec reconnexion automatique
"""

import json
import os
import signal
import sys
import time

import requests
from kafka import KafkaProducer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "wikipedia-edits"
WATCHED_PROJECTS = {
    "en.wikipedia.org", "fr.wikipedia.org",
    "de.wikipedia.org", "es.wikipedia.org", "ru.wikipedia.org"
}

count = 0
running = True


def shutdown(signum, frame):
    global running
    print(f"\nArrêt demandé. Total publié : {count} éditions.")
    running = False


def consume(producer):
    global count, running
    headers = {"User-Agent": "wikipedia-pulse/1.0 (bigdata-project)"}

    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=60) as r:
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
            if count % 50 == 0:
                print(f"  → {count} éditions publiées dans Kafka")


def main():
    global running
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"Producer connecté à Kafka ({KAFKA_BOOTSTRAP})")
    print(f"Topic : '{TOPIC}' | 5 langues : EN FR DE ES RU")
    print("Ctrl+C pour arrêter.\n")

    while running:
        try:
            consume(producer)
        except Exception as e:
            if not running:
                break
            print(f"  ⚠ Connexion coupée ({e.__class__.__name__}) — reconnexion dans 3s...")
            time.sleep(3)

    producer.flush()
    producer.close()
    print(f"Producer fermé proprement. Total : {count} éditions.")


if __name__ == "__main__":
    main()
