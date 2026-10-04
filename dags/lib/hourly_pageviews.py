"""
hourly_pageviews.py

Ingestion des pageviews HORAIRES par article depuis les dumps Wikimedia.

Source : https://dumps.wikimedia.org/other/pageviews/{YYYY}/{YYYY-MM}/pageviews-{YYYYMMDD}-{HH}0000.gz
  - ligne : "domain_code page_title count_views total_response_size"
  - trafic humain uniquement (agent "user")
  - le nom du fichier indique la FIN de l'heure : pageviews-20261002-120000.gz
    contient 11:00 → 12:00 UTC (vérifié contre l'API aggregate horaire)

Un fichier ≈ 60 Mo / 6,7 M lignes. On le lit en flux et on ne garde que :
  - les 5 langues suivies (desktop + mobile fusionnés)
  - les articles édités la veille ou le jour même (candidats du lead-lag)

Écriture : datalake/raw/wikimedia_dumps/PageviewsHourly/{YYYYMMDD}/pageviews_hourly.tsv.gz
           (hour_utc \t project \t title \t views)
"""

import gzip
import io
import os
import time
import zlib
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import urllib3

from lib.common import PROJECTS, USER_AGENT, target_date

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))
DUMPS_URL = "https://dumps.wikimedia.org/other/pageviews"
DOWNLOAD_TIMEOUT_S = 120
# Coupure réseau en plein téléchargement : on réessaie l'heure, pas toute la journée
MAX_ATTEMPTS = 3
RETRY_BACKOFF_S = 15
TRANSIENT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    urllib3.exceptions.HTTPError,  # ReadTimeoutError / ProtocolError pendant la lecture du flux
    EOFError,                      # gzip tronqué
    zlib.error,                    # gzip corrompu pendant le transfert
    gzip.BadGzipFile,
)

# Convention des dumps : titre "-" = total du projet. On l'écrit pour chaque langue
# et chaque heure : c'est le profil jour/nuit qui sert à désaisonnaliser les vues.
TOTAL_TITLE = "-"

# "fr" (desktop) et "fr.m" (mobile) → "fr.wikipedia"
DOMAIN_TO_PROJECT = {
    code: project
    for project in PROJECTS
    for code in (project.split(".")[0], project.split(".")[0] + ".m")
}


def raw_file(date: datetime) -> Path:
    return (
        DATALAKE_ROOT / "raw" / "wikimedia_dumps" / "PageviewsHourly"
        / date.strftime("%Y%m%d") / "pageviews_hourly.tsv.gz"
    )


def dump_url(hour_start: datetime) -> str:
    """URL du dump couvrant [hour_start, hour_start + 1h) — nommé d'après la fin de l'heure."""
    end = hour_start + timedelta(hours=1)
    return f"{DUMPS_URL}/{end:%Y}/{end:%Y-%m}/pageviews-{end:%Y%m%d-%H}0000.gz"


def parse_line(line: str):
    """(project, title, views) pour une ligne d'une langue suivie, sinon None."""
    parts = line.split(" ")
    if len(parts) < 3:
        return None
    project = DOMAIN_TO_PROJECT.get(parts[0])
    if project is None:
        return None
    try:
        return project, parts[1], int(parts[2])
    except ValueError:
        return None


def filter_hour(lines, candidates: set) -> dict:
    """Vues de l'heure par (project, title) candidat, desktop + mobile additionnés,
    plus le total de chaque langue sous (project, TOTAL_TITLE)."""
    views = Counter()
    for line in lines:
        parsed = parse_line(line)
        if parsed is None:
            continue
        project, title, count = parsed
        views[(project, TOTAL_TITLE)] += count
        if (project, title) in candidates:
            views[(project, title)] += count
    return dict(views)


def to_candidate(edit_project: str, title: str) -> tuple:
    """Clé au format des dumps : ("fr.wikipedia", "Tour_Eiffel")."""
    return edit_project.removesuffix(".org"), title.replace(" ", "_")


def load_candidates(date: datetime) -> set:
    """Articles édités la veille et le jour même (éditions formatées)."""
    candidates = set()
    for day in (date - timedelta(days=1), date):
        folder = (
            DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits"
            / day.strftime("%Y%m%d") / "edits.snappy.parquet"
        )
        if not folder.exists():
            print(f"  ⚠ Pas d'éditions formatées pour {day:%Y-%m-%d}")
            continue
        df = pd.read_parquet(folder, columns=["project", "title"]).drop_duplicates()
        candidates |= {to_candidate(p, t) for p, t in zip(df["project"], df["title"])}
    return candidates


@contextmanager
def open_dump(url: str):
    """Itérateur de lignes du dump, décompressé à la volée (rien sur disque)."""
    with requests.get(url, headers={"User-Agent": USER_AGENT}, stream=True,
                      timeout=DOWNLOAD_TIMEOUT_S) as response:
        response.raise_for_status()
        with gzip.GzipFile(fileobj=response.raw) as gz:
            yield io.TextIOWrapper(gz, encoding="utf-8", errors="replace")


def ensure_published(url: str) -> None:
    """Échoue tout de suite (HTTPError) si le dump n'est pas encore publié."""
    response = requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=30, allow_redirects=True)
    response.raise_for_status()


def read_hour(url: str, candidates: set) -> dict:
    """Lit et filtre un dump horaire ; réessaie les erreurs réseau transitoires.

    Un 404 (dump pas encore publié) remonte tout de suite : ce sont les retries
    Airflow, espacés d'une heure, qui attendent la publication.
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with open_dump(url) as lines:
                return filter_hour(lines, candidates)
        except TRANSIENT_ERRORS as e:
            if attempt == MAX_ATTEMPTS:
                raise
            print(f"  ⚠ {url} : {e.__class__.__name__}, nouvel essai {attempt + 1}/{MAX_ATTEMPTS}")
            time.sleep(RETRY_BACKOFF_S * attempt)


def hourly_pageviews_to_raw(**kwargs):
    """Point d'entrée Airflow.

    Le dump de la dernière heure du jour D est publié vers D+1 02:00 UTC :
    tant qu'il manque, la tâche échoue et les retries du DAG reprennent.
    """
    date = target_date(kwargs)
    print(f"=== hourly_pageviews_to_raw | {date:%Y-%m-%d} ===")

    candidates = load_candidates(date)
    if not candidates:
        raise RuntimeError("Aucun article édité : rien à suivre (éditions formatées manquantes ?)")
    print(f"  → {len(candidates)} articles candidats")

    # Le dernier dump est publié en dernier : s'il manque, inutile de lire les 23 autres
    ensure_published(dump_url(date + timedelta(hours=23)))

    output_file = raw_file(date)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = output_file.with_suffix(".tmp")

    total = 0
    try:
        with gzip.open(tmp_file, "wt", encoding="utf-8") as out:
            for hour in range(24):
                hour_start = date + timedelta(hours=hour)
                views = read_hour(dump_url(hour_start), candidates)
                for (project, title), count in views.items():
                    out.write(f"{hour_start:%Y-%m-%d %H:%M:%S}\t{project}\t{title}\t{count}\n")
                total += len(views)
                print(f"  → {hour_start:%H}h : {len(views)} articles vus")
        tmp_file.replace(output_file)  # écriture atomique : rerun = écrasement propre
    finally:
        tmp_file.unlink(missing_ok=True)

    print(f"  → {total} lignes écrites dans {output_file}")
    print("=== hourly_pageviews_to_raw done ===")
