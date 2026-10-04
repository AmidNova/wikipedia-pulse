# Wikipedia Pulse

Pipeline Big Data qui détecte les événements mondiaux émergents en croisant le flux temps réel des éditions Wikipedia (lead indicator) avec les pageviews quotidiennes (lag indicator).

## Sources de données

| #   | Source                       | Type            | Endpoint                                            |
| --- | ---------------------------- | --------------- | --------------------------------------------------- |
| 1   | Wikimedia EventStreams       | Streaming (SSE) | https://stream.wikimedia.org/v2/stream/recentchange |
| 2   | Wikimedia Analytics REST API | Batch quotidien | https://wikimedia.org/api/rest_v1/metrics/pageviews |
| 3   | Wikimedia Pageviews dumps    | Batch horaire   | https://dumps.wikimedia.org/other/pageviews/        |

Les deux APIs sont totalement gratuites, sans authentification.

## Architecture du Datalake

Convention de nommage imposée : `/{layer}/{group}/{TableName}/{date}/filename`

### Layers

- **raw** — données brutes telles que reçues des APIs (JSON)
- **formatted** — données normalisées en parquet, dates UTC, colonnes nettoyées
- **usage** — résultats finaux croisés, prêts à indexer dans Elasticsearch

### Groups

| Group                 | Description                                    |
| --------------------- | ---------------------------------------------- |
| `wikimedia_stream`    | Données issues du stream temps réel (source 1) |
| `wikimedia_analytics` | Données issues de l'API batch (source 2)       |
| `wikimedia_dumps`     | Pageviews horaires par article (source 3)      |
| `wikipediaPulse`      | Couche usage : résultat du croisement          |

### Tables

| Layer     | Group               | Table            | Contenu                                |
| --------- | ------------------- | ---------------- | -------------------------------------- |
| raw       | wikimedia_stream    | Edits            | Éditions brutes du flux SSE (NDJSON)   |
| raw       | wikimedia_analytics | Pageviews        | Pageviews brutes par article (JSON)    |
| formatted | wikimedia_stream    | Edits            | Éditions normalisées en parquet        |
| raw       | wikimedia_dumps     | PageviewsHourly  | Vues horaires filtrées (TSV gz)        |
| formatted | wikimedia_analytics | Pageviews        | Pageviews normalisées en parquet       |
| formatted | wikimedia_dumps     | PageviewsHourly  | Vues horaires en parquet (UTC)         |
| usage     | wikipediaPulse      | TrendingArticles | Articles détectés en émergence         |
| usage     | wikipediaPulse      | EditLeadLag      | Ratio effort d'édition / audience      |
| usage     | wikipediaPulse      | EditLeadLagHourly | Décalage édition → lecture à l'heure  |

### Exemple de chemin complet

## Structure du projet

wikipedia-pulse/
├── README.md # Ce fichier
├── docker-compose.yml # Stack : Airflow + Postgres + Elastic + Kibana
├── dags/ # DAGs Airflow
│ ├── wikipedia_pulse_dag.py # Pipeline principal
│ └── lib/ # Code Python métier (fetchers, transformers)
├── test/ # Tests isolés des fonctions (hors Airflow)
└── datalake/ # Stockage local des données
├── raw/
│ ├── wikimedia_stream/Edits/
│ └── wikimedia_analytics/Pageviews/
├── formatted/
│ ├── wikimedia_stream/Edits/
│ └── wikimedia_analytics/Pageviews/
└── usage/
└── wikipediaPulse/
├── TrendingArticles/
└── EditLeadLag/

## Pipeline Airflow (DAG)

edits_stream_to_raw ──> raw_to_formatted_edits ──┐
├──> produce_pulse ──> index_to_elastic
pageviews_to_raw ──> raw_to_formatted_pageviews ──┘

| Tâche                        | Rôle                                                                                 |
| ---------------------------- | ------------------------------------------------------------------------------------ |
| `edits_stream_to_raw`        | Consomme le flux SSE et écrit dans `raw/wikimedia_stream/Edits/`                     |
| `pageviews_to_raw`           | Appelle l'API batch et écrit dans `raw/wikimedia_analytics/Pageviews/`               |
| `raw_to_formatted_edits`     | Normalise les éditions en parquet                                                    |
| `raw_to_formatted_pageviews` | Normalise les pageviews en parquet                                                   |
| `produce_pulse`              | Croise les deux sources, détecte les anomalies (pic d'éditions), calcule le lead-lag |
| `hourly_pageviews_to_raw`    | Lit en flux les 24 dumps horaires, garde les articles édités (J-1, J)                |
| `raw_to_formatted_hourly_pageviews` | Normalise les vues horaires en parquet                                        |
| `produce_leadlag`            | Corrélation croisée horaire éditions × vues sur 48 h, pour le jour J-1               |
| `index_to_elastic`           | Pousse les résultats dans Elasticsearch                                              |

## Lead-lag horaire (EditLeadLagHourly)

Pour chaque article édité au moins 3 fois le jour E, on construit deux séries horaires
sur 48 h (E 00:00 → E+2 00:00 UTC) : éditions `e(t)` et vues `v(t)`, puis la corrélation
croisée `corr(e(t), v(t+k))` pour `k ∈ [-12 h, +24 h]`.

| Colonne            | Sens                                                              |
| ------------------ | ----------------------------------------------------------------- |
| `best_lag_hours`   | `k` qui maximise la corrélation (> 0 : les lectures suivent)      |
| `best_corr`        | Corrélation à ce décalage                                         |
| `peak_lag_hours`   | Heure du pic de vues − heure du pic d'éditions                    |
| `view_surge_ratio` | Pic de vues / médiane horaire des vues (intensité de l'attention) |
| `pattern`          | `edit_led`, `view_led`, `simultaneous` ou `decorrelated`          |

Le run du jour D traite E = D-1 (il faut les vues de D pour suivre les éditions de fin de journée).
Les dumps sont nommés d'après la **fin** de l'heure : `pageviews-20261002-120000.gz` = 11h–12h UTC.

> Avec peu d'éditions, la corrélation d'un article isolé est bruitée : interpréter les
> distributions (par langue, par jour) plutôt que chaque article.

## Stack technique

| Outil          | Rôle                                          |
| -------------- | --------------------------------------------- |
| Docker Compose | Orchestration de tous les services            |
| Apache Airflow | Orchestration du pipeline (DAGs)              |
| PostgreSQL     | Base de métadonnées Airflow                   |
| Python         | Ingestion + transformations (pandas, pyarrow) |
| Elasticsearch  | Indexation des résultats                      |
| Kibana         | Dashboard de visualisation                    |

## Lancement

```bash
docker compose up -d
```

Accès :

- Airflow : http://localhost:8080 (admin / admin)
- Kibana : http://localhost:5601
- Elasticsearch : http://localhost:9200

## Membres du groupe

- Amidou SORO
- Khaleb TCHOUMBOU
