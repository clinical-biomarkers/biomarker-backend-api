from pymongo.database import Database
from pymongo.errors import BulkWriteError
from pymongo import InsertOne
from typing import NoReturn, Optional, Literal
import sys
import os
from time import sleep
from datetime import datetime, timedelta
from stop_words import EN_STOP_WORDS

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils import ROOT_DIR
from tutils.logging import setup_logging, log_msg
from tutils.constants import biomarker_default, unreviewed_default, stats_default
from tutils.general import get_user_confirmation

LOGGER = setup_logging("load_data.log")
CHECKPOINT_FP = os.path.join(ROOT_DIR, "logs", "load_checkpoint_{server}.txt")
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


def clear_collections(dbh: Database, max_retries: int = 3) -> None:
    """Clears the biomarker and unreviewed collections."""
    for collection_name in list(TARGET_COLLECTIONS.values()):
        if collection_name == "stats":
            continue

        collection = dbh[collection_name]
        for attempt in range(max_retries):
            try:
                collection.delete_many({})
            except Exception as e:
                if attempt == max_retries - 1:
                    log_msg(
                        logger=LOGGER,
                        msg=f"Failed to clear {collection_name}: {e}",
                        level="error",
                    )
                    raise
                sleep_time = 2**(attempt + 1)
                log_msg(
                    logger=LOGGER,
                    msg=f"Failed to clear {collection_name} on attempt {attempt + 1} of {max_retries}, sleeping for {sleep_time} seconds...",
                    level="error",
                )
                log_msg(logger=LOGGER, msg=f"{e}\n", level="error")
                sleep(sleep_time)


def create_load_record_command(record: dict, all_text: bool = True) -> InsertOne:
    """Creates the into the biomarker collection."""
    if all_text:
        record["all_text"] = _concatenate_fields(document=record)
    return InsertOne(record)


def bulk_load(
    dbh: Database,
    ops: list[InsertOne],
    destination: Literal["biomarker", "collision"],
    server: str,
    current_index: int,
    max_retries: int = 3,
) -> None:
    """Performs a bulk write."""

    if not ops:
        return

    collection = dbh[TARGET_COLLECTIONS[destination]]
    log_msg(logger=LOGGER, msg=f"Bulk writing at index: {current_index + 1} ------")

    successful_ops = 0

    # First, try the entire batch
    try:
        result = collection.bulk_write(ops)

        if destination == "biomarker":
            new_index = current_index + len(ops)
            save_checkpoint(last_index=new_index, server=server)

        log_msg(
            logger=LOGGER,
            msg=f"Successfully completed bulk write: {result.inserted_count} documents inserted",
        )
        return

    except BulkWriteError as e:
        if hasattr(e, "details"):
            successful_ops = e.details.get("nInserted", 0)
            log_msg(
                logger=LOGGER,
                msg=f"Bulk write failed after inserting {successful_ops} documents. Falling back to individual inserts.",
                level="warning",
            )

            if destination == "biomarker" and successful_ops > 0:
                new_index = current_index + successful_ops
                save_checkpoint(last_index=new_index, server=server)
        else:
            log_msg(
                logger=LOGGER,
                msg="Bulk write failed. Falling back to individual inserts.",
                level="warning",
            )

    except Exception as e:
        log_msg(
            logger=LOGGER,
            msg=f"Unexpected error during bulk write: {str(e)}. Falling back to individual inserts.",
            level="warning",
        )

    # If we get here, the bulk insert failed - fall back to one-by-one insertion
    log_msg(logger=LOGGER, msg="Starting individual document insertion...")

    # Track where to start based on how many succeeded in the bulk operation
    start_idx = successful_ops if "successful_ops" in locals() else 0

    # Process remaining documents one by one
    for i in range(start_idx, len(ops)):
        for attempt in range(max_retries):
            try:
                result = collection.bulk_write([ops[i]])

                if destination == "biomarker":
                    new_index = current_index + start_idx + (i - start_idx) + 1
                    save_checkpoint(last_index=new_index, server=server)

                log_msg(
                    logger=LOGGER,
                    msg=f"Successfully inserted document {i + 1}/{len(ops)}",
                )
                break  # Success - move to next document

            except BulkWriteError as e:
                error_str = str(e)
                if "duplicate key error" in error_str.lower():
                    log_msg(
                        logger=LOGGER,
                        msg=f"Document {i + 1}/{len(ops)} is a duplicate - already exists in the database",
                        level="warning",
                    )
                    break  # Move to next document

                if attempt < max_retries - 1:
                    sleep_time = 2 ** (attempt + 2)
                    log_msg(
                        logger=LOGGER,
                        msg=f"Error inserting document {i + 1}/{len(ops)}. Retrying in {sleep_time} seconds (attempt {attempt + 1}/{max_retries})",
                        level="warning",
                    )
                    sleep(sleep_time)
                else:
                    log_msg(
                        logger=LOGGER,
                        msg=f"Failed to insert document {i + 1}/{len(ops)} after {max_retries} attempts. Error: {error_str}",
                        level="error",
                    )
                    raise  # Re-raise the exception after max retries

            except Exception as e:
                if attempt < max_retries - 1:
                    sleep_time = 2 ** (attempt + 2)
                    log_msg(
                        logger=LOGGER,
                        msg=f"Unexpected error inserting document {i + 1}/{len(ops)}: {str(e)}. Retrying in {sleep_time} seconds (attempt {attempt + 1}/{max_retries})",
                        level="warning",
                    )
                    sleep(sleep_time)
                else:
                    log_msg(
                        logger=LOGGER,
                        msg=f"Failed to insert document {i + 1}/{len(ops)} after {max_retries} attempts due to unexpected error: {str(e)}",
                        level="error",
                    )
                    raise  # Re-raise the exception after max retries

    log_msg(logger=LOGGER, msg=f"Completed processing all {len(ops)} documents")


def _concatenate_fields(document: dict, max_size: int = 10_000_000) -> str:
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
    filtered_cache: dict[str, str] = {}
    current_size = 0

    def filter_words(text: str) -> str:
        if text in filtered_cache:
            return filtered_cache[text]
        words = text.lower().strip().split()
        filtered = " ".join(w for w in words if w not in EN_STOP_WORDS)
        filtered_cache[text] = filtered
        return filtered

    def add_val(value: Optional[str]):
        nonlocal current_size
        if value is not None:
            filtered = filter_words(value)
            if filtered and filtered not in result_str:
                new_size = current_size + len(filtered)
                if new_size <= max_size:
                    result_str.append(filtered)
                    current_size = new_size
                else:
                    log_msg(
                        logger=LOGGER,
                        msg=f"Skipping addition of text for {document['biomarker_id']}: would exceed max size of {max_size} bytes",
                        level="warning",
                    )

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

    condition = document.get("condition")
    if condition is not None:
        cond_rec_name = condition.get("recommended_name", {})
        add_val(cond_rec_name.get("id", ""))
        add_val(cond_rec_name.get("name", ""))
        add_val(cond_rec_name.get("description", ""))
        add_val(cond_rec_name.get("resource", ""))
        for cond_syn in condition.get("synonyms", []):
            add_val(cond_syn["id"])
            add_val(cond_syn["name"])
            add_val(cond_syn["resource"])

    exposure_agent = document.get("exposure_agent")
    if exposure_agent is not None:
        exposure_agent_rec_name = exposure_agent.get("recommended_name", {})
        add_val(exposure_agent_rec_name.get("id", ""))
        add_val(exposure_agent_rec_name.get("name", ""))
        add_val(exposure_agent_rec_name.get("description", ""))
        add_val(exposure_agent_rec_name.get("resource", ""))

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


def load_checkpoint(server: str) -> int | NoReturn:
    try:
        with open(CHECKPOINT_FP.format(server=server), "r") as f:
            lines = f.readlines()
            if len(lines) != 2:
                log_msg(
                    logger=LOGGER,
                    msg=f"Invalid checkpoint file format at `{CHECKPOINT_FP}`",
                    level="warning",
                    to_stdout=True,
                )
                return -1

            timestamp_str = lines[0].strip()
            index = int(lines[1].strip())

            try:
                checkpoint_time = datetime.fromisoformat(timestamp_str)
                time_diff = datetime.now() - checkpoint_time

                if time_diff > timedelta(hours=3):
                    msg = (
                        f"Checkpoint is {time_diff.total_seconds() / 3600:.1f} hours old. "
                        f"Timestamp: {checkpoint_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        "Do you want to continue?"
                    )
                    log_msg(logger=LOGGER, msg=msg, level="warning", to_stdout=True)
                    get_user_confirmation()

                return index

            except ValueError as e:
                log_msg(
                    logger=LOGGER,
                    msg=f"Invalid timestamp format in checkpoint file: {e}",
                    level="warning",
                    to_stdout=True,
                )
                return -1

    except FileNotFoundError:
        log_msg(
            logger=LOGGER,
            msg=f"Couldn't find checkpoint file at `{CHECKPOINT_FP}`",
            level="warning",
        )
        return -1
    except Exception as e:
        log_msg(
            logger=LOGGER,
            msg=f"Error reading from checkpoint file at `{CHECKPOINT_FP}`\n{e}",
            level="error",
        )
        return -1


def save_checkpoint(last_index: int, server: str) -> None:
    timestamp = datetime.now().isoformat()
    with open(CHECKPOINT_FP.format(server=server), "w") as f:
        f.write(f"{timestamp}\n{last_index}")
    log_msg(logger=LOGGER, msg=f"Updated checkpoint: {last_index}")
