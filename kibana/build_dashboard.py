"""
build_dashboard.py

Génère kibana/wikipedia-pulse.ndjson : data views, recherche enregistrée et dashboard
"Wikipedia Pulse" (Lens). Les identifiants sont fixes : un nouvel import écrase
l'ancien au lieu de dupliquer.

    python kibana/build_dashboard.py          # régénère le fichier
    (le service compose kibana-setup l'importe au démarrage)
"""

import hashlib
import json
from pathlib import Path

OUTPUT = Path(__file__).with_name("wikipedia-pulse.ndjson")
DASHBOARD_ID = "wikipedia-pulse"

DATA_VIEWS = {
    "trending": ("wp-trending", "wikipedia-trending"),
    "crosslang": ("wp-crosslang", "wikipedia-crosslang"),
    "leadlag": ("wp-leadlag-hourly", "wikipedia-leadlag-hourly"),
}
CROSSLANG_SEARCH_ID = "wp-crosslang-events"

# Versions de schéma des objets (Kibana 8.13) : sans elles, l'import rejoue toutes les
# migrations depuis la 7.x, qui attendent l'ancien format Lens et plantent (500).
CORE_VERSION = "8.8.0"
TYPE_VERSIONS = {"index-pattern": "8.0.0", "dashboard": "8.9.0"}


# ─── Colonnes Lens ────────────────────────────────────────────────────────────

def count(label, kql=None):
    col = {"label": label, "dataType": "number", "operationType": "count", "isBucketed": False,
           "scale": "ratio", "sourceField": "___records___", "params": {"emptyAsNull": True},
           "customLabel": True}
    if kql:
        col["filter"] = {"query": kql, "language": "kuery"}
    return col


def max_of(field, label):
    return {"label": label, "dataType": "number", "operationType": "max", "isBucketed": False,
            "scale": "ratio", "sourceField": field, "params": {"emptyAsNull": True}, "customLabel": True}


def terms(field, label, size, order_by=None, data_type="string"):
    order = {"type": "column", "columnId": order_by} if order_by else {"type": "alphabetical", "fallback": False}
    return {"label": label, "dataType": data_type, "operationType": "terms", "isBucketed": True,
            "scale": "ordinal", "sourceField": field, "customLabel": True,
            "params": {"size": size, "orderBy": order, "orderDirection": "desc" if order_by else "asc",
                       "otherBucket": False, "missingBucket": False, "parentFormat": {"id": "terms"}}}


def per_day(label="Jour"):
    return {"label": label, "dataType": "date", "operationType": "date_histogram", "isBucketed": True,
            "scale": "interval", "sourceField": "date", "customLabel": True,
            "params": {"interval": "d", "includeEmptyRows": True, "dropPartials": False}}


# ─── Visualisations ───────────────────────────────────────────────────────────

def lens(view, title, vis_type, columns, visualization, kql=""):
    """Attributs Lens by-value : une couche, colonnes dans l'ordre donné."""
    layer = "layer-" + hashlib.sha1(title.encode("utf-8")).hexdigest()[:12]
    view_id = DATA_VIEWS[view][0]
    return {
        "title": title,
        "visualizationType": vis_type,
        "type": "lens",
        "references": [{"type": "index-pattern", "id": view_id, "name": f"indexpattern-datasource-layer-{layer}"}],
        "state": {
            "visualization": visualization(layer),
            "query": {"query": kql, "language": "kuery"},
            "filters": [],
            "datasourceStates": {"formBased": {"layers": {layer: {
                "columns": columns, "columnOrder": list(columns), "incompleteColumns": {}, "sampling": 1,
            }}}},
            "internalReferences": [],
            "adHocDataViews": {},
        },
    }


def metric(view, title, column):
    return lens(view, title, "lnsMetric", {"m": column},
                lambda layer: {"layerId": layer, "layerType": "data", "metricAccessor": "m"})


def stacked_bars(view, title, x, split, y, kql=""):
    return lens(view, title, "lnsXY", {"x": x, "split": split, "y": y}, lambda layer: {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "preferredSeriesType": "bar_stacked",
        "layers": [{"layerId": layer, "layerType": "data", "seriesType": "bar_stacked",
                    "xAccessor": "x", "splitAccessor": "split", "accessors": ["y"]}],
    }, kql)


def bars(view, title, x, y, kql=""):
    return lens(view, title, "lnsXY", {"x": x, "y": y}, lambda layer: {
        "legend": {"isVisible": False, "position": "right"},
        "valueLabels": "show",
        "preferredSeriesType": "bar",
        "layers": [{"layerId": layer, "layerType": "data", "seriesType": "bar",
                    "xAccessor": "x", "accessors": ["y"]}],
    }, kql)


def donut(view, title, group, size, kql=""):
    return lens(view, title, "lnsPie", {"g": group, "s": size}, lambda layer: {
        "shape": "donut",
        "layers": [{"layerId": layer, "layerType": "data", "primaryGroups": ["g"], "metrics": ["s"],
                    "numberDisplay": "percent", "categoryDisplay": "default", "legendDisplay": "default"}],
    }, kql)


def table(view, title, columns, sort_by, kql=""):
    return lens(view, title, "lnsDatatable", columns, lambda layer: {
        "layerId": layer, "layerType": "data",
        "columns": [{"columnId": c} for c in columns],
        "sorting": {"columnId": sort_by, "direction": "desc"},
    }, kql)


def panels():
    """(x, y, w, h, attributs Lens ou None pour la recherche enregistrée)."""
    emerging_table = table("trending", "Articles émergents (vs leur propre historique)", {
        "t": terms("title.keyword", "Article", 50, order_by="z"),
        "l": terms("edit_project.keyword", "Langue", 5, order_by="z"),
        "e": max_of("unique_editors", "Éditeurs"),
        "b": max_of("baseline_editors", "Habituel"),
        "z": max_of("z_editors", "z"),
        "v": max_of("pageviews", "Vues"),
        "n": max_of("language_count", "Langues"),
    }, sort_by="z", kql="is_emerging : true")
    return [
        (0, 0, 12, 6, metric("trending", "Articles édités", count("Articles édités"))),
        (12, 0, 12, 6, metric("trending", "Articles émergents", count("Émergents", "is_emerging : true"))),
        (24, 0, 12, 6, metric("crosslang", "Sujets multilingues", count("Sujets édités dans ≥ 2 langues"))),
        (36, 0, 12, 6, metric("leadlag", "Lectures qui suivent les éditions",
                              count("Lectures qui suivent les éditions", 'pattern.keyword : "edit_led"'))),
        (0, 6, 30, 16, emerging_table),
        (30, 6, 18, 16, stacked_bars("trending", "Émergents par jour et par langue",
                                     per_day(), terms("edit_project.keyword", "Langue", 5, order_by="y"),
                                     count("Émergents"), kql="is_emerging : true")),
        (0, 22, 30, 16, None),  # recherche enregistrée : événements multilingues
        (30, 22, 18, 16, bars("crosslang", "Quelle langue édite en premier ?",
                              terms("first_language.keyword", "Première langue", 5, order_by="y"),
                              count("Sujets multilingues"))),
        (0, 38, 30, 14, stacked_bars("leadlag", "Décalage édition → lecture (heures)",
                                     terms("best_lag_hours", "Décalage (h)", 40, data_type="number"),
                                     terms("pattern.keyword", "Motif", 4, order_by="y"),
                                     count("Articles"), kql='not pattern.keyword : "decorrelated"')),
        (30, 38, 18, 14, donut("leadlag", "Qui mène : édition ou lecture ?",
                               terms("pattern.keyword", "Motif", 4, order_by="s"), count("Articles"))),
    ]


# ─── Objets enregistrés ───────────────────────────────────────────────────────

def data_view(view_id, index):
    return {"type": "index-pattern", "id": view_id,
            "attributes": {"title": index, "name": index, "timeFieldName": "date"}, "references": []}


def crosslang_search():
    return {
        "type": "search", "id": CROSSLANG_SEARCH_ID,
        "attributes": {
            "title": "Événements multilingues",
            "columns": ["titles", "language_count", "first_language", "spread_minutes",
                        "sum_editors", "total_pageviews", "emerging_languages"],
            "sort": [["language_count", "desc"], ["sum_editors", "desc"]],
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                "query": {"query": "", "language": "kuery"}, "filter": [],
                "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
            })},
        },
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                        "type": "index-pattern", "id": DATA_VIEWS["crosslang"][0]}],
    }


def dashboard():
    panels_json, references = [], []
    for i, (x, y, w, h, attrs) in enumerate(panels()):
        panel_id = f"p{i}"
        panel = {"gridData": {"x": x, "y": y, "w": w, "h": h, "i": panel_id}, "panelIndex": panel_id}
        if attrs is None:
            panel.update(type="search", panelRefName=f"panel_{panel_id}", embeddableConfig={})
            references.append({"name": f"{panel_id}:panel_{panel_id}", "type": "search", "id": CROSSLANG_SEARCH_ID})
        else:
            config = {"attributes": attrs, "enhancements": {}}
            if attrs["visualizationType"] == "lnsMetric":  # la tuile affiche déjà son libellé
                config["hidePanelTitles"] = True
            panel.update(type="lens", embeddableConfig=config)
            references += [{**ref, "name": f"{panel_id}:{ref['name']}"} for ref in attrs["references"]]
        panels_json.append(panel)
    return {
        "type": "dashboard", "id": DASHBOARD_ID,
        "attributes": {
            "title": "Wikipedia Pulse",
            "description": "Émergence par article, propagation entre langues, décalage édition → lecture",
            "panelsJSON": json.dumps(panels_json),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "syncCursor": True,
                                       "syncTooltips": False, "hidePanelTitles": False}),
            "timeRestore": True, "timeFrom": "now-30d/d", "timeTo": "now",
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(
                {"query": {"query": "", "language": "kuery"}, "filter": []})},
        },
        "references": references,
    }


def with_versions(obj):
    version = TYPE_VERSIONS.get(obj["type"])
    return {**obj, "coreMigrationVersion": CORE_VERSION, "typeMigrationVersion": version} if version else obj


def main():
    objects = [data_view(*v) for v in DATA_VIEWS.values()] + [crosslang_search(), dashboard()]
    objects = [with_versions(o) for o in objects]
    OUTPUT.write_text("".join(json.dumps(o, ensure_ascii=False) + "\n" for o in objects), encoding="utf-8")
    print(f"{len(objects)} objets → {OUTPUT}")


if __name__ == "__main__":
    main()
