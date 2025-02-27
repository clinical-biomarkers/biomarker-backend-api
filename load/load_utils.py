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


def clear_collections(dbh: Database, max_retries: int = 3, delay: float = 1.0) -> None:
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
                log_msg(
                    logger=LOGGER,
                    msg=f"Failed to clear {collection_name} on attempt {attempt + 1} of {max_retries}, sleeping for {delay} seconds...",
                    level="error",
                )
                log_msg(logger=LOGGER, msg=f"{e}\n", level="error")
                sleep(delay)


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
    current_index: int = -1,
    batch_size: int = 100,
    max_retries: int = 3,
) -> None:
    """Performs a bulk write."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    collection = dbh[TARGET_COLLECTIONS[destination]]
    successful_ops = 0

    # First try writing all ops at once
    try:
        collection.bulk_write(ops)
        if destination == "biomarker":
            save_checkpoint(last_index=current_index, server=server)
        return
    except BulkWriteError as e:
        msg = "Bulk write error on entire batch write attempt" + ("-" * 50) + "\n"
        level: Literal["warning", "error"] = "warning"

        if "duplicate key error" in str(e).lower():
            msg = (
                "Encountered duplicate key error in bulk write. This likely means some records "
                "were already inserted in a previous attempt and this script will fail."
            )
            level = "error"

        if hasattr(e, "details"):
            successful_ops = e.details.get("nInserted", 0)
            if destination == "biomarker" and successful_ops > 0:
                save_checkpoint(
                    last_index=current_index - (len(ops) - successful_ops),
                    server=server,
                )
            msg += f"\tSuccessfully grabbed `nInserted` from BulkWriteError exception: {successful_ops}\n"
        else:
            msg += f"\tCould not grab `nInserted` from BulkWriteError exception\n"
            level = "error"

        msg += f"Falling back to smaller batched writes\nERROR DETAILS: {e}\n\n"
        log_msg(logger=LOGGER, msg=msg, level=level)
    except Exception as e:
        msg = (
            "Bulk write failed on non-BulkWriteError, continuing but will likely "
            "fail with a duplicate key error in subsequent smaller batch writes\n"
            f"ERROR DETAILS: {e}\n\n"
        )
        log_msg(
            logger=LOGGER,
            msg=msg,
            level="error",
        )

    remaining_ops = ops[successful_ops:]
    log_msg(logger=LOGGER, msg=f"{len(remaining_ops)} remaining operations")

    # If entire bulk write fails, try smaller batches with retries
    for i in range(0, len(remaining_ops), batch_size):
        batch = remaining_ops[i : i + batch_size]
        log_msg(logger=LOGGER, msg=(f"Starting batch {i} attempt" + ("-" * 50)))

        for attempt in range(max_retries):
            try:
                result = collection.bulk_write(batch)
                batch_successful = result.inserted_count
                if destination == "biomarker":
                    new_index = (
                        current_index
                        - (len(ops) - successful_ops)
                        + i
                        + batch_successful
                    )
                    save_checkpoint(last_index=new_index, server=server)
                successful_ops += batch_successful
                log_msg(
                    logger=LOGGER,
                    msg=f"Successfully wrote remaining operations on batch {i + 1}",
                )
                break
            except BulkWriteError as e:
                if "duplicate key error" in str(e).lower():
                    msg = (
                        f"Batch at index {i}: Encountered duplicate key error. This likely means "
                        f"some records were already inserted and this will fail.\nERROR DETAILS: {e}\n\n"
                    )
                    log_msg(logger=LOGGER, msg=msg, level="error")

                if hasattr(e, "details"):
                    batch_successful = e.details.get("nInserted", 0)
                    successful_ops += batch_successful
                    if destination == "biomarker":
                        new_index = current_index - (len(ops) - successful_ops)
                        save_checkpoint(last_index=new_index, server=server)

                    if attempt == max_retries - 1:
                        log_msg(
                            logger=LOGGER,
                            msg=f"Failed all {max_retries} attempts for batch starting at index {i}\nERROR DETAILS: {e}\n\n",
                            level="error",
                        )
                        raise
                    sleep_time = 2**attempt
                    log_msg(
                        logger=LOGGER,
                        msg=f"Batch index {i}: Attempt {attempt + 1} of {max_retries} failed, retrying in {sleep_time} seconds:\nERROR DETAILS: {e}\n\n",
                        level="warning",
                    )
                    sleep(sleep_time)
            except Exception as e:
                if attempt == max_retries - 1:
                    log_msg(
                        logger=LOGGER,
                        msg=f"Failed with non-BulkWriteError all {max_retries} attempts for batch starting at index {i}:\nERROR DETAILS: {e}\n\n",
                        level="error",
                    )
                    raise
                sleep_time = 2**attempt
                msg = (
                    f"Batch at index {i}: Attempt {attempt + 1} of {max_retries} failed with non-BulkWriteError, "
                    f"retrying in {sleep_time} seconds:\nERROR DETAILS: {e}\n\n"
                )
                log_msg(
                    logger=LOGGER,
                    msg=msg,
                    level="warning",
                )
                sleep(sleep_time)


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
