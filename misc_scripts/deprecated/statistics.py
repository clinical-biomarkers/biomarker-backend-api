""" DEPRECATED

Handles the logic for updating the supplemental metadata collections.

From id/helpers
"""

import logging
from pymongo.database import Database
from typing import List, Dict

COLLECTION_DEFAULT = "biomarker_collection"
STATS_COLLECTION = "stats_collection"
UNIQUE_COND_COUNT = [{"$group": {"_id": "$condition.id"}}]
# calculates the single and multicomponent counts
COMPONENT_COUNTS: List[Dict] = [
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
ENTITY_TYPE_SPLITS: List[Dict] = [
    {"$unwind": "$biomarker_component"},
    {
        "$group": {
            "_id": "$biomarker_component.assessed_entity_type",
            "count": {"$sum": 1},
        }
    },
    {"$project": {"entity_type": "$_id", "count": 1, "_id": 0}},
]


def process_stats(
    dbh: Database,
    collection: str = COLLECTION_DEFAULT,
    stat_collection: str = STATS_COLLECTION,
) -> bool:
    """Entry point for processing the statistics and splits for the data.

    Parameters
    ---------
    dbh : Database
        The database handle.
    collection : str, optional
        The collection to calculate stats from.
    stat_collection : str, optional
        The collection to store the calculated stats.

    Returns
    -------
    bool
        True on success, False on any type of failure.
    """
    try:
        unique_condition_count = _count_documents(dbh, UNIQUE_COND_COUNT)
        unique_biomarker_count = len(dbh[COLLECTION_DEFAULT].distinct("biomarker_id"))

        component_counts = list(
            dbh[collection].aggregate(COMPONENT_COUNTS, allowDiskUse=True)
        )
        single_biomarker_count = (
            component_counts[0]["single_biomarker_count"] if component_counts else 0
        )
        multicomponent_biomarker_count = (
            component_counts[0]["multicomponent_biomarker_count"]
            if component_counts
            else 0
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
            dbh[collection].aggregate(ENTITY_TYPE_SPLITS, allowDiskUse=True)
        )
        dbh[stat_collection].replace_one(
            {"_id": "entity_type_splits"},
            {"_id": "entity_type_splits", "splits": entity_type_splits},
            upsert=True,
        )

        logging.info("Statistics calculated successfully.")
        return True

    except Exception as e:
        logging.error(f"Error processing statistics: {e}")
        print(f"Error processing statistics: {e}")
        return False


def _count_documents(
    dbh: Database, pipeline: List[Dict], collection: str = COLLECTION_DEFAULT
) -> int:
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
