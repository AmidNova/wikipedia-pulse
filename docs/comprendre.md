# Comprendre le projet — les vraies explications

> Doc perso. Pas le rendu officiel. C'est le truc à lire pour piger ce qu'on a fait
> et pourquoi, et pour pouvoir l'expliquer à quelqu'un.

---

## 1. C'est quoi le projet, en une phrase honnête

On construit une chaîne automatique qui va chercher des données sur Wikipedia,
les nettoie, les croise, et sort un résultat utile. Le prof appelle ça un
"pipeline de données". C'est tout. Le reste c'est de la plomberie autour de cette idée.

Le sujet précis qu'on a choisi : détecter les sujets "qui montent" sur Wikipedia.

---

## 2. L'idée derrière le sujet (le truc à comprendre AVANT la technique)

Quand un truc se passe dans le monde (un mort célèbre, un match, une élection),
voilà ce qui arrive sur Wikipedia, dans cet ordre :

1. D'ABORD : les contributeurs Wikipedia se jettent sur l'article pour le mettre
   à jour. Ils sont rapides, ils suivent l'actu.
2. ENSUITE : le grand public arrive pour LIRE l'article. Mais ça, ça vient après,
   quand l'info s'est répandue.

Donc il y a un décalage : les éditions montent AVANT les lectures.

Notre projet mesure ce décalage. Si on voit un article qui se fait éditer comme un
fou mais qui n'est pas encore beaucoup lu => c'est probablement un événement en
train d'émerger. On le détecte avant qu'il devienne viral.

En vocabulaire : les éditions = "lead indicator" (signal en avance),
les lectures = "lag indicator" (signal en retard). D'où le nom du projet : Pulse,
le pouls de l'actu.

---

## 3. Les deux sources de données (le prof en exige 2 minimum)

Le but d'un datalake c'est de CROISER des données. Donc il faut 2 sources.

SOURCE 1 — les éditions Wikipedia, en temps réel
  Wikipedia publie un flux en direct de TOUTES les modifications faites sur le site,
  partout dans le monde, en continu. Ça s'appelle EventStreams. C'est un "flux" :
  les données arrivent une par une, sans fin, comme un robinet ouvert.

SOURCE 2 — les pages les plus lues, par jour
  Wikipedia publie aussi, chaque jour, le classement des articles les plus consultés.
  Ça c'est du "batch" : une grosse photo une fois par jour, pas un flux continu.

Une source en flux + une source en batch = exactement ce que le prof demande
(une source doit se rafraîchir souvent).

Pourquoi seulement Anglais + Français ? Parce que Wikipedia a 300 langues. Si on
prend tout, ça fait trop de données pour nos machines, et surtout le croisement
devient impossible (un article s'appelle "Paris" en français mais autrement en
chinois — impossible à matcher). EN + FR c'est suffisant et propre.

---

## 4. C'est quoi un Datalake et pourquoi cette structure de dossiers

Un datalake c'est juste une façon ORGANISÉE de ranger les données. Au lieu de tout
balancer en vrac, on range en 3 étages :

  raw       = les données BRUTES, exactement comme l'API nous les donne.
              On touche à rien. C'est notre "sauvegarde de base".
  formatted = les données NETTOYÉES. Colonnes propres, dates au bon format,
              converties en parquet (un format efficace pour l'analyse).
  usage     = le RÉSULTAT FINAL, les données croisées, prêtes à être affichées.

Pourquoi 3 étages ? Parce que si un jour un calcul foire, on sait exactement où
chercher le problème. Si raw est bon mais formatted est faux => le bug est dans
le nettoyage. Sans les étages, on cherche à l'aveugle.

La règle de nommage imposée par le prof :
  /{layer}/{group}/{table}/{date}/fichier
  exemple : raw/wikimedia_stream/Edits/20260521/edits.ndjson
Faut la respecter à la lettre, ça vaut 1 point bonus ("clean naming").

---

## 5. La stack — chaque outil, et POURQUOI on l'a pris

La "stack" = la liste des outils qui tournent ensemble. Le piège c'est de pas
comprendre à quoi sert chaque truc. Voilà cash :

DOCKER COMPOSE
  Le truc qui lance tout le reste. Sans lui, faudrait installer Airflow, Postgres,
  Kafka, etc. un par un à la main = l'enfer. Docker met chaque outil dans une
  "boîte" isolée (un container) et Docker Compose lance toutes les boîtes d'un coup
  avec une seule commande.

AIRFLOW
  Le chef d'orchestre. C'est lui qui décide : "d'abord tu vas chercher les données,
  ENSUITE tu les nettoies, ENSUITE tu les croises". Il lance les étapes dans le bon
  ordre, au bon moment, et si une étape plante il peut réessayer.
  Un "DAG" = juste le schéma de ces étapes et de leur ordre.

POSTGRESQL
  Une base de données. Mais attention : elle ne stocke PAS nos données Wikipedia.
  Elle sert juste à Airflow pour noter SON travail à lui (quelles étapes ont tourné,
  les logs, l'historique). C'est le carnet de notes interne d'Airflow.

REDIS
  Une file d'attente. Quand Airflow a plein de tâches à faire, Redis les met en
  file et les distribue. Détail technique, pas besoin d'en faire un plat.

KAFKA
  LE point important. Kafka c'est un tampon. Le flux Wikipedia coule en continu
  24h/24. Mais notre pipeline, lui, ne tourne pas en continu. Problème.
  Solution : un programme (le "producer") écoute le flux Wikipedia non-stop et
  balance tout dans Kafka. Kafka GARDE les données en attente. Quand notre pipeline
  se réveille, il lit dans Kafka ce qui s'est accumulé. Kafka fait le pont entre
  "le flux qui coule tout le temps" et "le pipeline qui tourne de temps en temps".
  Sans Kafka, on ratait des données entre deux exécutions.

SPARK
  Le moteur de calcul. Quand on a beaucoup de données à nettoyer ou croiser, Spark
  fait ça vite parce qu'il découpe le travail en morceaux et les traite en parallèle.
  On l'utilise pour le nettoyage (formatted) et le croisement (usage).
  Détail : Spark est écrit en Java, donc on a dû mettre Java dans nos boîtes Docker.
  Nous on l'utilise en Python, ça s'appelle "PySpark" — on écrit du Python, et un
  pont traduit vers le moteur Java.

ELASTICSEARCH (pas encore fait)
  Une base de données spéciale, super rapide pour la recherche. On y mettra le
  résultat final.

KIBANA (pas encore fait)
  L'écran de visualisation. Branché sur Elasticsearch, il affiche des graphes,
  des cartes, des tableaux. C'est ce que le prof verra à la fin.

---

## 6. Le chemin d'une donnée, du début à la fin

Suis une édition Wikipedia depuis sa naissance jusqu'au dashboard :

ÉTAPE 1 — quelqu'un modifie un article sur Wikipedia
  Wikipedia publie ça dans son flux EventStreams.

ÉTAPE 2 — notre producer Kafka attrape l'édition
  Le script kafka/producer.py écoute le flux en continu. Il attrape l'édition,
  vire ce qui nous intéresse pas (les bots, les langues autres que EN/FR), et
  pousse l'édition dans Kafka.

ÉTAPE 3 — le consumer lit Kafka et écrit en raw
  Quand le pipeline tourne, edits_consumer.py vide Kafka et écrit toutes les
  éditions dans raw/ (en NDJSON, un format où chaque ligne est une édition).

ÉTAPE 4 — en parallèle, on récupère les pages les plus lues
  pageviews_fetcher.py appelle l'autre API et écrit le classement dans raw/ (JSON).

ÉTAPE 5 — on nettoie les deux sources (Spark)
  Les deux formatters lisent le raw, nettoient (dates au bon format, colonnes
  propres), et écrivent en formatted/ au format parquet.

ÉTAPE 6 — on croise (Spark)
  pulse_combiner.py prend les éditions nettoyées ET les pages lues nettoyées,
  les croise par titre d'article, et calcule :
  - combien de fois chaque article a été édité
  - un "score de tendance" (beaucoup d'éditions + peu de lectures = ça monte)
  Résultat écrit dans usage/.

ÉTAPE 7 — on indexe et on affiche (pas encore fait)
  On poussera le résultat dans Elasticsearch, et Kibana l'affichera en dashboard.

Tout ça est enchaîné automatiquement par Airflow (le DAG). Une seule commande
lance toute la chaîne.

---

## 7. Les galères qu'on a eues (et comment on les a réglées)

C'est utile à savoir, ça peut retomber en question.

- Le flux SSE qui coupait : au début on lisait le flux Wikipedia directement,
  mais la connexion coupait au bout de quelques secondes dans Docker.
  Réglé en passant par Kafka.

- Spark qui refusait les dates : Spark version 4 n'accepte pas les dates écrites
  en "nanosecondes". On a forcé l'écriture en "microsecondes".

- Spark qui trouvait pas Java : on a dû dire explicitement à Spark où était Java
  dans le code.

- Deux dossiers qui divergeaient : au début on avait un dossier "projet" et un
  dossier "airflow" séparés, on copiait des fichiers de l'un à l'autre = bordel.
  Réglé en fusionnant tout dans un seul dossier, mis sur GitHub.

---

## 8. Ce qui reste à faire

- index_to_elastic : pousser le résultat dans Elasticsearch (on attend le cours)
- Le dashboard Kibana (on attend le cours)
- Le PDF de présentation (10 pages max)
- La vidéo (10 min max)

---

## 9. Si on te pose LA question en soutenance

"Expliquez votre projet" => 
On a fait un pipeline de données qui détecte les sujets d'actualité émergents.
On croise deux signaux Wikipedia : les éditions en temps réel (les contributeurs
réagissent vite) et les lectures quotidiennes (le public arrive après). Le décalage
entre les deux révèle ce qui est en train de monter. Techniquement : ingestion via
Kafka pour le temps réel, nettoyage et croisement avec Spark, orchestration avec
Airflow, le tout rangé dans un datalake en 3 couches, et exposé dans un dashboard
Kibana.

Voilà. Si tu peux dire ça sans lire, t'as compris ton projet.
