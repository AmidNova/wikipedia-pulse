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
from datetime import datetime
from pathlib import Path

import requests

from lib.common import PROJECTS, USER_AGENT, target_date

DATALAKE_ROOT = Path("/opt/airflow/datalake")
WIKIMEDIA_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews"


def fetch_top_pageviews(project: str, date: datetime) -> dict:
    """Appelle l'API Wikimedia Analytics pour récupérer les top articles lus."""
    year  = date.strftime("%Y")
    month = date.strftime("%m")
    day   = date.strftime("%d")

    url = f"{WIKIMEDIA_API}/top/{project}/all-access/{year}/{month}/{day}"
    headers = {"User-Agent": USER_AGENT}

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

    Le run planifié du jour D est lancé à D+1 00:00 : on récupère les pageviews
    de D. L'API publie avec quelques heures de retard → les retries du DAG
    prennent le relais tant que la donnée n'est pas disponible.
    """
    date = target_date(kwargs)
    print(f"=== pageviews_to_raw | target date : {date.strftime('%Y-%m-%d')} ===")

    failures = []
    for project in PROJECTS:
        try:
            data = fetch_top_pageviews(project, date)
            save_to_raw(data, project, date)
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Request error for {project}: {e}")
            failures.append(project)

    if failures:
        raise RuntimeError(f"Pageviews non récupérées pour : {', '.join(failures)}")
    print("=== pageviews_to_raw done ===")
