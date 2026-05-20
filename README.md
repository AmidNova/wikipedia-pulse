# Wikipedia Pulse

Pipeline Big Data qui détecte les événements mondiaux émergents en croisant le flux temps réel des éditions Wikipedia (lead indicator) avec les pageviews quotidiennes (lag indicator).

## Sources de données

| #   | Source                       | Type            | Endpoint                                            |
| --- | ---------------------------- | --------------- | --------------------------------------------------- |
| 1   | Wikimedia EventStreams       | Streaming (SSE) | https://stream.wikimedia.org/v2/stream/recentchange |
| 2   | Wikimedia Analytics REST API | Batch quotidien | https://wikimedia.org/api/rest_v1/metrics/pageviews |

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
| `wikipediaPulse`      | Couche usage : résultat du croisement          |

### Tables

| Layer     | Group               | Table            | Contenu                                |
| --------- | ------------------- | ---------------- | -------------------------------------- |
| raw       | wikimedia_stream    | Edits            | Éditions brutes du flux SSE (NDJSON)   |
| raw       | wikimedia_analytics | Pageviews        | Pageviews brutes par article (JSON)    |
| formatted | wikimedia_stream    | Edits            | Éditions normalisées en parquet        |
| formatted | wikimedia_analytics | Pageviews        | Pageviews normalisées en parquet       |
| usage     | wikipediaPulse      | TrendingArticles | Articles détectés en émergence         |
| usage     | wikipediaPulse      | EditLeadLag      | Décalage édition → lecture par article |

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
| `index_to_elastic`           | Pousse les résultats dans Elasticsearch                                              |

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
