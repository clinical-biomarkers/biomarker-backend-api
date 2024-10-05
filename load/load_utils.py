from pymongo.database import Database
from pymongo import InsertOne
from typing import Optional, Literal
import subprocess
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.constants import biomarker_default, unreviewed_default, stats_default

TARGET_COLLECTIONS = {
    "biomarker": biomarker_default(),
    "collision": unreviewed_default(),
    "stats": stats_default(),
}
UNIQUE_COND_COUNT = [{"$group": {"_id": "$condition.id"}}]
COMPONENT_COUNTS: list[dict] = [
    {"$project": {"component_count": {"$size": "$biomarker_component"}}},
    {
        "$group": {
            "_id": None,
            "single_biomarker_count": {
                "$sum": {"$cond": [{"$eq": ["$component_count", 1]}, 1, 0]}
            },
            "multicomponent_biomarker_count": {
                "$sum": {"$cond": [{"$gt": ["$component_count", 1]}, 1, 0]}
            },
        }
    },
]
ENTITY_TYPE_SPLITS: list[dict] = [
    {"$unwind": "$biomarker_component"},
    {
        "$group": {
            "_id": "$biomarker_component.assessed_entity_type",
            "count": {"$sum": 1},
        }
    },
    {"$project": {"entity_type": "$_id", "count": 1, "_id": 0}},
]


def clear_collections(dbh: Database) -> None:
    """Clears the biomarker and unreviewed collections."""
    for collection_name in list(TARGET_COLLECTIONS.values()):
        if collection_name == "stats":
            continue
        collection = dbh[collection_name]
        collection.delete_many({})


def create_load_record_command(record: dict, all_text: bool = True) -> InsertOne:
    """Creates the into the biomarker collection."""
    if all_text:
        record["all_text"] = _concatenate_fields(document=record)
    return InsertOne(record)


def bulk_load(
    dbh: Database, ops: list[InsertOne], destination: Literal["biomarker", "collision"]
) -> None:
    """Performs a bulk write."""
    collection = dbh[TARGET_COLLECTIONS[destination]]
    collection.bulk_write(ops)


def _concatenate_fields(document: dict) -> str:
    """Concatenates the relevant string fields in the data model into one string for the text index field.

    Parameters
    ----------
    document : dict
        The data model document.

    Returns
    -------
    str
        The concatenated string.
    """

    def add_val(value: Optional[str]):
        if value is not None:
            value = f"{value.lower().strip()}"
            if value not in result_str:
                result_str.append(value)

    result_str: list[str] = []
    add_val(document["biomarker_id"])
    add_val(document["biomarker_canonical_id"])

    components = document["biomarker_component"]
    for component in components:
        add_val(component["biomarker"])
        add_val(component["assessed_biomarker_entity"]["recommended_name"])
        for entity_syn in component["assessed_biomarker_entity"].get("synonyms", []):
            add_val(entity_syn["synonym"])
        add_val(component["assessed_biomarker_entity_id"])
        add_val(component["assessed_entity_type"])
        for specimen in component.get("specimen", []):
            add_val(specimen["name"])
            add_val(specimen["id"])
            add_val(specimen["name_space"])
            add_val(specimen["loinc_code"])
        for comp_evidence in component.get("evidence_source", []):
            add_val(comp_evidence["id"])
            add_val(comp_evidence["database"])
            for evidence in comp_evidence.get("evidence_list", []):
                add_val(evidence["evidence"])
    for role in document["best_biomarker_role"]:
        add_val(role["role"])
    add_val(document["condition"]["recommended_name"]["id"])
    add_val(document["condition"]["recommended_name"]["name"])
    add_val(document["condition"]["recommended_name"]["description"])
    add_val(document["condition"]["recommended_name"]["resource"])
    for cond_syn in document["condition"].get("synonyms", []):
        add_val(cond_syn["id"])
        add_val(cond_syn["name"])
        add_val(cond_syn["resource"])
    for top_evidence in document.get("evidence_source", []):
        add_val(top_evidence["id"])
        add_val(top_evidence["database"])
        for evidence in top_evidence.get("evidence_list", []):
            add_val(evidence["evidence"])
    for citation in document.get("citation", []):
        add_val(citation["title"])
        add_val(citation["journal"])
        add_val(citation["authors"])
        for reference in citation.get("reference", []):
            add_val(reference["id"])
            add_val(reference["type"])

    return " ".join(result_str)


def process_stats(
    dbh: Database,
    data_collection: str = TARGET_COLLECTIONS["biomarker"],
    stat_collection: str = TARGET_COLLECTIONS["stats"],
) -> None:
    """Entry point for processing the statistics and splits for the data.

    Parameters
    ---------
    dbh: Database
        The database handle.
    data_collection: str, optional
        The collection to calculate stats from.
    stat_collection: str, optional
        The collection to store the calculated stats.
    """
    unique_condition_count = _count_documents(
        dbh=dbh, pipeline=UNIQUE_COND_COUNT, collection=data_collection
    )
    unique_biomarker_count = len(dbh[data_collection].distinct("biomarker_id"))

    component_counts = list(
        dbh[data_collection].aggregate(COMPONENT_COUNTS, allowDiskUse=True)
    )
    single_biomarker_count = (
        component_counts[0]["single_biomarker_count"] if component_counts else 0
    )
    multicomponent_biomarker_count = (
        component_counts[0]["multicomponent_biomarker_count"] if component_counts else 0
    )

    stats = {
        "unique_condition_count": unique_condition_count,
        "unique_biomarker_count": unique_biomarker_count,
        "single_biomarker_count": single_biomarker_count,
        "multicomponent_biomarker_count": multicomponent_biomarker_count,
    }
    dbh[stat_collection].replace_one(
        {"_id": "stats"}, {"_id": "stats", **stats}, upsert=True
    )

    entity_type_splits = list(
        dbh[data_collection].aggregate(ENTITY_TYPE_SPLITS, allowDiskUse=True)
    )
    dbh[stat_collection].replace_one(
        {"_id": "entity_type_splits"},
        {"_id": "entity_type_splits", "splits": entity_type_splits},
        upsert=True,
    )


def _count_documents(dbh: Database, pipeline: list[dict], collection: str) -> int:
    """Performs the aggregation pipeline and returns the resulting count.

    Parameters
    ----------
    dbh : Database
        The database handle.
    pipeline: list[dict]
        The aggregation pipeline to run.
    collection : str, optional
        The collection to run the pipeline on.

    Returns
    -------
    int
        The resulting condition count.
    """
    result = list(
        dbh[collection].aggregate(pipeline + [{"$count": "count"}], allowDiskUse=True)
    )
    return result[0]["count"] if result else 0
