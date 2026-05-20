# Wikipedia Pulse — État du projet

> Dernière mise à jour : 21 mai 2026

## Sujet

Pipeline Big Data qui détecte les événements mondiaux émergents en croisant le flux temps réel des éditions Wikipedia (lead indicator) avec les pageviews quotidiennes (lag indicator).

## Membres
- Amidou SORO
- Khaleb TCHOUMBOU

## Dépôt
https://github.com/AmidNova/wikipedia-pulse

## Sources de données
| # | Source | Type | Endpoint |
|---|--------|------|----------|
| 1 | Wikimedia EventStreams | Streaming SSE | https://stream.wikimedia.org/v2/stream/recentchange |
| 2 | Wikimedia Analytics REST API | Batch J+1 | https://wikimedia.org/api/rest_v1/metrics/pageviews |

Choix EN + FR uniquement (volume gérable, matching propre). Les pageviews sont à J+1 : le pipeline croise les éditions du jour J avec les pageviews J-1.

## Stack technique
| Outil | Rôle |
|-------|------|
| Docker Compose | Lance toute l'infrastructure |
| Airflow 2.9.2 (CeleryExecutor) | Orchestre le pipeline (DAGs) |
| PostgreSQL | État interne d'Airflow |
| Redis | File de tâches Airflow |
| Spark 4.1.1 (PySpark) | Formatting + combination |
| Kafka 3.7.0 (KRaft) | Tampon temps réel du flux d'éditions |
| Elasticsearch | Indexation des résultats (à venir) |
| Kibana | Dashboard (à venir) |

## Architecture Datalake
Convention : /{layer}/{group}/{TableName}/{YYYYMMDD}/filename

3 couches : raw (brut) -> formatted (parquet) -> usage (résultats croisés).

## DAG Airflow 
DAG branché et lancé : run complet en SUCCESS de bout en bout (sauf index_to_elastic qui est un placeholder).

## État des tâches
| # | Tâche | Statut |
|---|-------|--------|
| 1 | pageviews_to_raw | OK Fait |
| 2 | raw_to_formatted_pageviews (Spark) | OK Fait |
| 3 | edits via Kafka (producer + consumer) | OK Fait |
| 4 | raw_to_formatted_edits (Spark) | OK Fait |
| 5 | produce_pulse (Spark) | OK Fait |
| 6 | DAG complet branché et testé | OK Fait |
| 7 | index_to_elastic | EN ATTENTE du cours Elasticsearch |
| 8 | Dashboard Kibana | EN ATTENTE du cours Kibana |
| 9 | PDF + vidéo de présentation | À faire |

## Barème — où on en est
Obligatoire (cellules vertes) :
- Ingestion 2 sources (2 pts) : FAIT
- Formatting parquet Spark (2 pts) : FAIT
- Combination join (2 pts) : FAIT
- Indexing Elastic (2 pts) : EN ATTENTE
- Dashboard Kibana (2 pts) : EN ATTENTE

Bonus déjà acquis :
- Realtime via Kafka (+1)
- Normalisation date UTC (+1)
- Spark formatting (+1)
- Spark combination (+0,5)
- Clean naming convention (+1)
- Run all in once (+1)

## Prochaine séance
- index_to_elastic (après cours Elasticsearch)
- Dashboard Kibana (après cours Kibana)
- ML dans la combination (bonus anomaly detection)
- PDF + vidéo
