# Wikipedia Pulse

Pipeline Big Data qui détecte les événements mondiaux émergents en croisant le flux temps réel des éditions Wikipedia (lead indicator) avec les pageviews quotidiennes (lag indicator).

## Sources de données

| #   | Source                       | Type            | Endpoint                                            |
| --- | ---------------------------- | --------------- | --------------------------------------------------- |
| 1   | Wikimedia EventStreams       | Streaming (SSE) | https://stream.wikimedia.org/v2/stream/recentchange |
| 2   | Wikimedia Analytics REST API | Batch quotidien | https://wikimedia.org/api/rest_v1/metrics/pageviews |
| 3   | Wikimedia Pageviews dumps    | Batch horaire   | https://dumps.wikimedia.org/other/pageviews/        |
| 4   | API MediaWiki (pageprops)    | Batch quotidien | https://{lang}.wikipedia.org/w/api.php              |

Toutes les sources sont gratuites, sans authentification.

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
| `wikimedia_api`       | QID Wikidata des articles édités (source 4)    |
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
| raw       | wikimedia_api       | Wikidata         | QID par article édité (NDJSON)         |
| formatted | wikimedia_api       | Wikidata         | QID par article édité en parquet       |
| usage     | wikipediaPulse      | TrendingArticles | Articles détectés en émergence         |
| usage     | wikipediaPulse      | CrossLanguageEvents | Entités éditées dans ≥ 2 langues    |
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
| `produce_pulse`              | Croise éditions, vues du jour et QID ; détecte les émergents vs leur historique      |
| `wikidata_to_raw`            | Relie chaque article édité à son QID Wikidata (API pageprops, 50 titres/requête)     |
| `raw_to_formatted_wikidata`  | Normalise les QID en parquet                                                         |
| `hourly_pageviews_to_raw`    | Lit en flux les 24 dumps horaires, garde les articles édités (J-1, J)                |
| `raw_to_formatted_hourly_pageviews` | Normalise les vues horaires en parquet                                        |
| `produce_leadlag`            | Corrélation croisée horaire éditions × vues sur 48 h, pour le jour J-1               |
| `index_to_elastic`           | Pousse les résultats dans Elasticsearch                                              |

`produce_pulse` attend les dumps horaires (publiés vers J+1 02:00) : les vues du jour
viennent de là, pour **tous** les articles édités. L'endpoint `top` de l'API ne couvre que
les 1000 articles les plus lus (~2 % des articles édités) : il ne sert plus qu'au rang.

## Détection des émergents (TrendingArticles)

Chaque article est comparé à **son propre** historique, pas aux autres articles du jour.
Sur les jours observés de [D-28, D-1] (jours où le pipeline a tourné ; 0 si l'article n'a
pas été édité), on prend la médiane et la MAD du nombre d'éditeurs distincts :

    z_editors = (éditeurs_D − médiane) / max(1.4826 × MAD, 1)

| Colonne            | Sens                                                                    |
| ------------------ | ----------------------------------------------------------------------- |
| `baseline_editors` | Médiane des éditeurs/jour sur l'historique                              |
| `z_editors`        | Écart robuste au comportement habituel (= `anomaly_score`)              |
| `z_edits`          | Même calcul sur le nombre d'éditions                                    |
| `history_days`     | Jours d'historique disponibles                                          |
| `is_emerging`      | `z_editors ≥ 3.5` et au moins 3 éditeurs distincts                      |

Le nombre d'émergents varie avec l'actualité (l'ancienne Isolation Forest en marquait
toujours 10 %). Sous 3 jours d'historique, aucun article n'est marqué. Le plancher à 1
évite qu'un article jamais édité devienne « infiniment anormal » pour un seul éditeur :
pour ces articles, `z_editors` vaut simplement le nombre d'éditeurs.

## Signature multilingue (Wikidata)

Chaque article est relié à son QID Wikidata (*Tour Eiffel* FR et *Eiffel Tower* EN → `Q243`).
Les langues sont regroupées par entité, plus par titre : *Paris* EN et *Paris* DE ne sont
pas confondus s'ils ne désignent pas la même chose.

| Colonne (TrendingArticles) | Sens                                                         |
| -------------------------- | ------------------------------------------------------------ |
| `wikidata_id`              | QID de l'article (vide si page sans élément Wikidata)        |
| `language_count`           | Nombre de langues qui ont édité l'entité ce jour             |
| `languages_editing`        | Langues dans l'ordre de leur première édition                |
| `first_language`           | Langue qui a édité en premier                                |
| `language_lag_minutes`     | Retard de cette langue sur la première                       |

`CrossLanguageEvents` (index `wikipedia-crosslang`) résume une ligne par entité éditée dans
au moins 2 langues : `titles`, `spread_minutes` (écart entre la première et la dernière
langue), totaux d'éditions et de vues, `sum_editors` (somme des éditeurs distincts de chaque
langue : un même compte actif dans deux langues compte deux fois), `emerging_languages`.

Wikidata est un enrichissement : si l'API est en panne, `produce_pulse` tourne quand même
(`trigger_rule="all_done"`) avec un regroupement article par article, et le run reste
marqué en échec via la tâche `wikidata_to_raw`.

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

## Contrôles qualité

Chaque tâche vérifie sa sortie et échoue **avant** d'écrire si les données sont manifestement
fausses (`dags/lib/quality.py`) :

| Couche              | Contrôle                                                   | Panne visée                          |
| ------------------- | ---------------------------------------------------------- | ------------------------------------ |
| Edits (formatted)   | ≥ 1 000 éditions, 5 langues, ≥ 99 % datées du jour         | Producer arrêté, dossiers décalés    |
| TrendingArticles    | ≥ 80 % d'articles avec vues dans chaque langue             | Jointure des vues cassée (top 1000)  |
| TrendingArticles    | (langue, titre) unique ; ≤ 5 % d'émergents                 | Doublons de jointure, seuil emballé  |
| Wikidata            | ≥ 50 % d'articles avec QID                                 | Réponse API dégradée                 |

## Tests

```bash
docker exec airflow-airflow-worker-1 python -m pytest /opt/airflow/test/unit -q
```

La CI GitHub Actions (`.github/workflows/ci.yml`) lance les mêmes tests à chaque push, avec les
dépendances épinglées de `requirements.txt` (aussi installées dans l'image).

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
cp .env.example .env   # puis renseigner FERNET_KEY (commande dans le fichier)
docker compose up -d
```

Le service `kibana-setup` importe le dashboard **Wikipedia Pulse** (`kibana/wikipedia-pulse.ndjson`)
dès que Kibana est prêt. Pour le modifier : éditer `kibana/build_dashboard.py`, puis

```bash
python kibana/build_dashboard.py && docker compose up kibana-setup
```

Les mappings Elasticsearch viennent du template d'index `wikipedia-pulse` (motif `wikipedia-*`),
posé par l'indexer avant chaque écriture : `date` est un vrai champ date (`yyyyMMdd`), les textes
ont un sous-champ `.keyword` pour les agrégations.

Accès :

- Airflow : http://localhost:8080 (admin / admin)
- Kibana : http://localhost:5601
- Elasticsearch : http://localhost:9200

## Membres du groupe

- Amidou SORO
- Khaleb TCHOUMBOU
