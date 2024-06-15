"""Loops through all the data in the biomarker collection and
creates the entries for the search collection.
"""

import pymongo
from pymongo.database import Database
import traceback
import sys
import argparse
import json
from typing import Set, Dict
from create_concat_field import concatenate_fields

LOG_BATCH_SIZE = 1_000


def get_dbh(host: str, db_name: str, db_user: str, db_pass: str) -> Database:
    """Get the MongoDB database handle.

    Parameters
    ----------
    host : str
        Host string.
    db_name : str
        The database name.
    db_user : str
        The username to connect with.
    db_pass : str
        The corresponding passowrd with the user.

    Returns
    -------
    Database
        The database handle.
    """
    try:
        client: pymongo.MongoClient = pymongo.MongoClient(
            host,
            username=db_user,
            password=db_pass,
            authSource=db_name,
            authMechanism="SCRAM-SHA-1",
            serverSelectionTimeoutMS=1000,
        )
        client.server_info()
        dbh = client[db_name]
        return dbh
    except Exception as e:
        print(e)
        sys.exit(1)


def update_search_collection(
    dbh: Database, source_collection: str, target_collection: str
) -> bool:
    """Loops over the source documents in the biomarker collection
    and builds the formatted entries for the search optimized colleciton.

    Parameters
    ----------
    dbh : Database
        The database handle.
    source_collection : str
        The source (biomarker) collection.
    target_collection : str
        The target (search) collection.

    Returns
    -------
    bool
        True on success, False on failure at any point.
    """
    cursor = dbh[source_collection].find()
    return_value = True
    for idx, document in enumerate(cursor):

        if (idx) % LOG_BATCH_SIZE == 0:
            print(f"Hit log checkpoint on idx {idx}")

        try:

            components = document["biomarker_component"]

            # top level fields
            biomarker_id = document["biomarker_id"]
            roles: Set[str] = set()
            role_counts: Dict[str, int] = {}
            for role_obj in document["best_biomarker_role"]:
                role = role_obj["role"]
                roles.add(role)
                if role in role_counts:
                    role_counts[role] += 1
                else:
                    role_counts[role] = 1
            condition_id = document["condition"]["recommended_name"]["id"]
            condition_syn_ids = [
                synonym["id"] for synonym in document["condition"]["synonyms"]
            ]
            condition_ids_comb = [condition_id] + condition_syn_ids
            condition_name = document["condition"]["recommended_name"]["name"]
            condition_syn_names = [
                synonym["name"] for synonym in document["condition"]["synonyms"]
            ]
            condition_names_comb = [condition_name] + condition_syn_names
            top_pmids: Set[str] = set()
            for evidence in document["evidence_source"]:
                top_pmids.add(evidence["id"])

            # component fields
            biomarker = [comp["biomarker"] for comp in components]
            assessed_biomarker_entity = [
                comp["assessed_biomarker_entity"]["recommended_name"]
                for comp in components
            ]
            assessed_biomarker_entity_syns = [
                synonym["synonym"]
                for comp in components
                for synonym in comp["assessed_biomarker_entity"].get("synonyms", [])
            ]
            assessed_biomarker_entity_comb = (
                [assessed_biomarker_entity] + assessed_biomarker_entity_syns
            )
            biomarker_entity_ids = [
                comp["assessed_biomarker_entity_id"] for comp in components
            ]
            entity_type_counts: Dict[str, int] = {}
            assessed_entity_types: Set[str] = set()
            for comp in components:
                entity_type = comp["assessed_entity_type"].lower().strip()
                assessed_entity_types.add(entity_type)
                if entity_type in entity_type_counts:
                    entity_type_counts[entity_type] += 1
                else:
                    entity_type_counts[entity_type] = 1
            specimen_names = [
                specimen["name"] for comp in components for specimen in comp["specimen"]
            ]
            specimen_ids = [
                specimen["id"] for comp in components for specimen in comp["specimen"]
            ]
            loinc_codes = [
                specimen["loinc_code"]
                for comp in components
                for specimen in comp["specimen"]
            ]
            comp_pmids: Set[str] = set()
            comp_evidence_sources = [comp["evidence_source"] for comp in components]
            for evidence in comp_evidence_sources:
                comp_pmids.add(evidence["id"])

            pmids_comb = list(top_pmids) + list(comp_pmids)

            # text index field
            all_text_field = concatenate_fields(document)

        except Exception as e:
            print(f"Caught:\n{e}")
            traceback.print_exc()
            return_value = False
            continue

        if not return_value:
            return return_value

        formatted_entry = {
            "biomarker_id": biomarker_id,
            "biomarkers": biomarker,
            "assessed_biomarker_entity": assessed_biomarker_entity_comb,
            "assessed_biomarker_entity_id": biomarker_entity_ids,
            "assessed_entity_type": assessed_entity_types,
            "entity_type_counts": entity_type_counts,
            "specimen_names": specimen_names,
            "specimen_ids": specimen_ids,
            "loinc_codes": loinc_codes,
            "roles": roles,
            "role_counts": role_counts,
            "condition_id": condition_ids_comb,
            "condition_names": condition_names_comb,
            "evidence_ids": pmids_comb,
            "all_text": all_text_field,
        }

        target_collection_handle = dbh[target_collection]
        try:
            target_collection_handle.delete_many(
                {"biomarker_id": formatted_entry["biomarker_id"]}
            )
            result = target_collection_handle.insert_one(formatted_entry)
            if result.inserted_id is None:
                print(f"Insert failed for biomarker_id: {biomarker_id}")
                print(f"formatted_entry: {formatted_entry}")
                return_value = False
        except Exception as e:
            print(f"Update error: {e}")
            traceback.print_exc()
            return_value = False

    return return_value


def main():
    parser = argparse.ArgumentParser(prog="update_search_collection.py")
    parser.add_argument("server", help="tst/prd")
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(0)
    server = options.server.lower().strip()
    if server not in {"tst", "prd"}:
        print("Invalid server name. Expects `tst` or `prd`.")
        sys.exit(0)

    config_obj = json.load(open("../api/config.json", "r"))
    port = config_obj["dbinfo"]["port"][server]
    host = f"mongodb://127.0.0.1:{port}"
    db_name = config_obj["dbinfo"]["dbname"]
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]
    dbh = get_dbh(host, db_name, db_user, db_pass)
    biomarker_collection_name = config_obj["dbinfo"][db_name]["collection"]
    search_collection_name = config_obj["dbinfo"][db_name]["search_collection"]

    if update_search_collection(dbh, biomarker_collection_name, search_collection_name):
        print("success")
    else:
        print("failure")


if __name__ == "__main__":
    main()
