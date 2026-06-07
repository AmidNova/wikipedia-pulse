"""
pageviews_fetcher.py

Récupère les top articles les plus lus du jour précédent sur Wikipedia
via l'API Wikimedia Analytics REST.

Endpoint :
    GET https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{project}/all-access/{year}/{month}/{day}

Sortie :
    datalake/raw/wikimedia_analytics/Pageviews/{YYYYMMDD}/pageviews.json
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

DATALAKE_ROOT = Path("/opt/airflow/datalake")
WIKIMEDIA_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews"
PROJECTS = ["en.wikipedia", "fr.wikipedia", "de.wikipedia", "es.wikipedia", "ru.wikipedia"]


def fetch_top_pageviews(project: str, date: datetime) -> dict:
    """Appelle l'API Wikimedia Analytics pour récupérer les top articles lus."""
    year  = date.strftime("%Y")
    month = date.strftime("%m")
    day   = date.strftime("%d")

    url = f"{WIKIMEDIA_API}/top/{project}/all-access/{year}/{month}/{day}"
    headers = {"User-Agent": "wikipedia-pulse/1.0 (bigdata-project)"}

    print(f"Fetching pageviews for {project} on {year}-{month}-{day}...")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    print(f"  → {len(data['items'][0]['articles'])} articles récupérés")
    return data


def save_to_raw(data: dict, project: str, date: datetime) -> Path:
    """Stocke le JSON brut dans la couche raw du datalake."""
    date_str = date.strftime("%Y%m%d")
    # Convention : /raw/{group}/{TableName}/{date}/filename
    output_dir = DATALAKE_ROOT / "raw" / "wikimedia_analytics" / "Pageviews" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)

    # On préfixe par le project pour avoir un fichier par source
    filename = f"pageviews_{project.replace('.', '_')}.json"
    output_file = output_dir / filename

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"  → Saved to {output_file}")
    return output_file


def pageviews_to_raw(**kwargs):
    """Point d'entrée Airflow.
    On récupère les pageviews du jour précédent (J-1) car l'API
    ne fournit pas encore les données du jour courant.
    """
    # On accède à la date du dag_run, jamais date.today()
    execution_date = kwargs["dag_run"].execution_date
    target_date = execution_date - timedelta(days=1)
    # On normalise en UTC
    target_date = target_date.replace(tzinfo=timezone.utc)

    print(f"=== pageviews_to_raw | target date : {target_date.strftime('%Y-%m-%d')} ===")

    for project in PROJECTS:
        try:
            data = fetch_top_pageviews(project, target_date)
            save_to_raw(data, project, target_date)
        except requests.exceptions.HTTPError as e:
            print(f"  ✗ HTTP error for {project}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Request error for {project}: {e}")

    print("=== pageviews_to_raw done ===")