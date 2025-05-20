from tutils.db import get_collections


def biomarker_default() -> str:
    collections = get_collections()
    return collections["data_model"]


def canonical_id_default() -> str:
    collections = get_collections()
    return collections["canonical_id_map"]


def second_level_id_default() -> str:
    collections = get_collections()
    return collections["second_level_id_map"]


def unreviewed_default() -> str:
    collections = get_collections()
    return collections["unreviewed"]


def stats_default() -> str:
    collections = get_collections()
    return collections["stats"]


def cache_default() -> str:
    collections = get_collections()
    return collections["cache"]


def log_default() -> str:
    collections = get_collections()
    return collections["req_log"]


def error_default() -> str:
    collections = get_collections()
    return collections["error_log"]


def ontology_default() -> str:
    collections = get_collections()
    return collections["ontology"]

def version_default() -> str:
    collections = get_collections()
    return collections["version"]

def event_default() -> str:
    collections = get_collections()
    return collections["event"]
