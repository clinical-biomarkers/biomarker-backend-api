from tutils.config import get_config


def biomarker_default() -> str:
    config = get_config()
    db_name = config["dbinfo"]["dbname"]
    return config["dbinfo"][db_name]["collection"]


def canonical_id_default() -> str:
    config = get_config()
    db_name = config["dbinfo"]["dbname"]
    return config["dbinfo"][db_name]["canonical_id_map"]


def second_level_id_default() -> str:
    config = get_config()
    db_name = config["dbinfo"]["dbname"]
    return config["dbinfo"][db_name]["second_level_id_map"]


def unreviewed_default() -> str:
    config = get_config()
    db_name = config["dbinfo"]["dbname"]
    return config["dbinfo"][db_name]["unreviewed_collection"]


def stats_default() -> str:
    config = get_config()
    db_name = config["dbinfo"]["dbname"]
    return config["dbinfo"][db_name]["stats_collection"]
