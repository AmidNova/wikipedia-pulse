"""Test isolé — exécuter directement, pas via Airflow."""
import sys
from pathlib import Path
from datetime import datetime, timezone

# Pour importer depuis dags/lib sans Airflow
sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))

from lib.pageviews_fetcher import fetch_top_pageviews, save_to_raw

# Override du datalake root pour les tests locaux
import lib.pageviews_fetcher as pf
pf.DATALAKE_ROOT = Path(__file__).parent.parent / "datalake"

if __name__ == "__main__":
    date = datetime(2026, 5, 18, tzinfo=timezone.utc)
    for project in ["en.wikipedia", "fr.wikipedia"]:
        data = fetch_top_pageviews(project, date)
        save_to_raw(data, project, date)
    print("\nTest OK — vérifie le dossier datalake/raw/wikimedia_analytics/Pageviews/")