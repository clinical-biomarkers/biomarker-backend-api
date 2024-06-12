"""Loops through all the data in the biomarker collection and
creates a concatenated sub field for text indexing.
"""

import pymongo
import sys
import argparse
import json
from typing import List


def concatenate_fields(document: dict) -> str:
    """Concatenates the relevant string fields in the data model into one string.

    Parameters
    ----------
    document : dict
        The data model document.

    Returns
    -------
    str
        The concatenated string.
    """

    def add_val(value: str):
        value = f"{value.lower().strip()}"
        if value not in result_str:
            result_str.append(value)

    result_str: List[str] = []
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


def main():

    parser = argparse.ArgumentParser(prog="create_concat_field.py")
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
    biomarker_collection_name = config_obj["dbinfo"][db_name]["collection"]

    batch_size = 1_000

    try:
        client = pymongo.MongoClient(
            host,
            username=db_user,
            password=db_pass,
            authSource=db_name,
            authMechanism="SCRAM-SHA-1",
            serverSelectionTimeoutMS=1000,
        )
        client.server_info()
        dbh = client[db_name]
        biomarker_collection = dbh[biomarker_collection_name]

        cursor = biomarker_collection.find()
        for idx, document in enumerate(cursor):
            if (idx + 1) % batch_size == 0:
                print(f"Hit log checkpoint on idx {idx}")
            concatenated_string = concatenate_fields(document)
            biomarker_collection.update_one(
                {"_id": document["_id"]}, {"$set": {"all_text": concatenated_string}}
            )
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
