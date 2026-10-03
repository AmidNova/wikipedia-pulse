"""
common.py

Constantes et helpers partagés par toutes les tâches du pipeline.
"""

from datetime import datetime, timezone

# Langues suivies — même liste pour le stream, l'API pageviews et les formatters
PROJECTS = ["en.wikipedia", "fr.wikipedia", "de.wikipedia", "es.wikipedia", "ru.wikipedia"]

# Politique User-Agent Wikimedia : un UA sans contact est limité à 10 req/min
USER_AGENT = "wikipedia-pulse/1.0 (https://github.com/AmidNova/wikipedia-pulse)"


def target_date(context) -> datetime:
    """Jour traité par le run : début de l'intervalle de données, à minuit UTC.

    Run planifié @daily du jour D (lancé à D+1 00:00) → D.
    Run manuel déclenché le jour D → D-1 (dernier intervalle complet).
    """
    start = context["data_interval_start"]
    return datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
