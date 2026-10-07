"""
wikidata_fetcher.py

Associe chaque article édité à son identifiant Wikidata (QID) : "Tour Eiffel" sur
FR et "Eiffel Tower" sur EN deviennent la même entité Q243. C'est ce qui permet
de compter les langues qui parlent d'un même sujet et de suivre sa propagation.

Endpoint (une langue, 50 titres max par requête, en POST : 50 titres cyrilliques
encodés dans l'URL d'un GET dépassent la limite et renvoient 414) :
    POST https://{lang}.wikipedia.org/w/api.php?action=query&prop=pageprops
        &ppprop=wikibase_item&redirects=1&titles=A|B|...

La réponse renomme les titres (normalisation, redirections) : on remonte la chaîne
pour rattacher le QID au titre tel qu'il apparaît dans les éditions.

Écritures :
    raw/wikimedia_api/Wikidata/{YYYYMMDD}/wikibase_items.ndjson
    formatted/wikimedia_api/Wikidata/{YYYYMMDD}/wikibase_items.snappy.parquet
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from lib.common import USER_AGENT, target_date

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))

BATCH_SIZE = 50                # limite de l'API pour un client non-bot
MIN_REQUEST_INTERVAL_S = 0.35  # ≤ ~170 req/min, sous la limite d'un UA identifié
MAX_ATTEMPTS = 5
RETRY_BACKOFF_S = 5
# Bases répliquées en retard : l'API refuse poliment, on attend (étiquette Wikimedia)
MAXLAG_S = 5
REQUEST_TIMEOUT_S = 30
# En dessous, la réponse de l'API est suspecte (≈ 97 % des articles ont un QID en pratique)
MIN_QID_RATE = 0.5


class TransientAPIError(Exception):
    """Erreur passagère côté Wikimedia : 429, maxlag, 5xx, réponse tronquée."""

    def __init__(self, message: str, retry_after: float = 0):
        super().__init__(message)
        self.retry_after = retry_after


def _retry_after(response) -> float:
    try:
        return float(response.headers.get("Retry-After", 0))
    except (TypeError, ValueError):
        return 0


def raw_file(date: datetime) -> Path:
    return (
        DATALAKE_ROOT / "raw" / "wikimedia_api" / "Wikidata"
        / date.strftime("%Y%m%d") / "wikibase_items.ndjson"
    )


def formatted_file(date: datetime) -> Path:
    return (
        DATALAKE_ROOT / "formatted" / "wikimedia_api" / "Wikidata"
        / date.strftime("%Y%m%d") / "wikibase_items.snappy.parquet"
    )


def api_url(edit_project: str) -> str:
    """"fr.wikipedia.org" → https://fr.wikipedia.org/w/api.php"""
    return f"https://{edit_project}/w/api.php"


def batches(items: list, size: int = BATCH_SIZE):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def resolve_items(titles: list, query: dict) -> dict:
    """{titre demandé: QID ou None} à partir du bloc "query" de la réponse.

    Un titre peut être normalisé puis redirigé : "paris_(France)" → "Paris (France)" → "Paris".
    """
    renames = {r["from"]: r["to"] for r in query.get("normalized", [])}
    renames.update({r["from"]: r["to"] for r in query.get("redirects", [])})
    qids = {
        page["title"]: page.get("pageprops", {}).get("wikibase_item")
        for page in query.get("pages", [])
    }

    resolved = {}
    for title in titles:
        final, seen = title, set()
        while final in renames and final not in seen:  # garde-fou contre une boucle de redirections
            seen.add(final)
            final = renames[final]
        resolved[title] = qids.get(final)
    return resolved


def parse_response(response, edit_project: str) -> dict:
    """Bloc "query" de la réponse ; TransientAPIError si un nouvel essai a du sens."""
    if response.status_code == 429 or response.status_code >= 500:
        raise TransientAPIError(f"HTTP {response.status_code}", _retry_after(response))
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as e:  # corps tronqué ou page HTML d'erreur
        raise TransientAPIError(f"réponse illisible : {e}") from e
    error = payload.get("error")
    if error and error.get("code") == "maxlag":
        raise TransientAPIError(f"maxlag : {error.get('info')}", _retry_after(response))
    if error:
        raise RuntimeError(f"API {edit_project} : {error.get('code')} — {error.get('info')}")
    if "continue" in payload:  # 50 titres ne devraient jamais être paginés : ne pas perdre la suite en silence
        raise RuntimeError(f"API {edit_project} : réponse paginée inattendue {payload['continue']}")
    return payload.get("query", {})


def query_batch(session: requests.Session, edit_project: str, titles: list) -> dict:
    """QID des titres d'un lot ; réessaie 429, maxlag, 5xx et erreurs réseau transitoires."""
    params = {
        "action": "query", "format": "json", "formatversion": "2",
        "prop": "pageprops", "ppprop": "wikibase_item", "redirects": "1",
        "maxlag": str(MAXLAG_S), "titles": "|".join(titles),
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session.post(api_url(edit_project), data=params, timeout=REQUEST_TIMEOUT_S)
            return resolve_items(titles, parse_response(response, edit_project))
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, TransientAPIError) as e:
            if attempt == MAX_ATTEMPTS:
                raise
            wait = max(RETRY_BACKOFF_S * attempt, getattr(e, "retry_after", 0))
            print(f"  ⚠ {edit_project} : {e}, nouvel essai dans {wait}s ({attempt + 1}/{MAX_ATTEMPTS})")
            time.sleep(wait)


def load_edited_articles(date: datetime) -> pd.DataFrame:
    folder = (
        DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits"
        / date.strftime("%Y%m%d") / "edits.snappy.parquet"
    )
    if not folder.exists():
        raise FileNotFoundError(f"Éditions formatées introuvables : {folder}")
    return pd.read_parquet(folder, columns=["project", "title"]).drop_duplicates()


def fetch_wikibase_items(articles: pd.DataFrame):
    """Génère {project, title, wikidata_id} pour chaque article, langue par langue."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    last_request = 0.0
    for edit_project, group in articles.groupby("project"):
        titles = sorted(group["title"].dropna().unique())
        found = 0
        for batch in batches(titles):
            pause = MIN_REQUEST_INTERVAL_S - (time.monotonic() - last_request)
            if pause > 0:
                time.sleep(pause)
            last_request = time.monotonic()
            for title, qid in query_batch(session, edit_project, batch).items():
                found += qid is not None
                yield {"project": edit_project, "title": title, "wikidata_id": qid}
        print(f"  → {edit_project} : {found}/{len(titles)} articles reliés à Wikidata")


def wikidata_to_raw(**kwargs):
    """Point d'entrée Airflow : QID des articles édités le jour D."""
    date = target_date(kwargs)
    print(f"=== wikidata_to_raw | {date:%Y-%m-%d} ===")

    articles = load_edited_articles(date)
    print(f"  → {len(articles)} articles à relier")

    output_file = raw_file(date)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = output_file.with_suffix(".tmp")
    try:
        with open(tmp_file, "w", encoding="utf-8") as out:
            for row in fetch_wikibase_items(articles):
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp_file.replace(output_file)  # écriture atomique : rerun = écrasement propre
    finally:
        tmp_file.unlink(missing_ok=True)
    print("=== wikidata_to_raw done ===")


def raw_to_formatted_wikidata(**kwargs):
    """Point d'entrée Airflow : NDJSON brut → parquet (project, title, wikidata_id)."""
    date = target_date(kwargs)
    print(f"=== raw_to_formatted_wikidata | {date:%Y-%m-%d} ===")

    input_file = raw_file(date)
    if not input_file.exists():
        raise FileNotFoundError(f"QID bruts introuvables : {input_file}")
    with open(input_file, encoding="utf-8") as f:
        rows = [json.loads(line) for line in f]
    df = pd.DataFrame(rows, columns=["project", "title", "wikidata_id"]).astype("string")
    df = df.drop_duplicates(["project", "title"])
    qid_rate = df["wikidata_id"].notna().mean() if len(df) else 0
    if qid_rate < MIN_QID_RATE:
        raise RuntimeError(f"Seulement {qid_rate:.0%} des articles ont un QID dans {input_file} : réponse API suspecte")

    output_file = formatted_file(date)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_file, index=False)
    print(f"  → {df['wikidata_id'].notna().sum()}/{len(df)} articles avec QID → {output_file}")
    print("=== raw_to_formatted_wikidata done ===")
