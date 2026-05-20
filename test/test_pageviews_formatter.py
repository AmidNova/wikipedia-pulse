"""Test isolé du formatter pageviews."""
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))

from lib.pageviews_formatter import convert_pageviews
import lib.pageviews_formatter as pf

pf.DATALAKE_ROOT = Path(__file__).parent.parent / "datalake"

if __name__ == "__main__":
    date = datetime(2026, 5, 18, tzinfo=timezone.utc)
    for project in ["en_wikipedia", "fr_wikipedia"]:
        convert_pageviews(project, date)
    print("\nTest OK — vérifie datalake/formatted/wikimedia_analytics/Pageviews/")