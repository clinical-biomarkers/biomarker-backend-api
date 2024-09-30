from typing import Optional
import os


def attempt_merge(merge_record: dict, collision_record: dict) -> Optional[dict]:
    """Attempts to merge a collision record and the source record.

    Merge criteria are as follows:
        - Any differences in these fields will cause the merge to fail:
            - biomarker_component.biomarker
            - biomarker_component.assessed_biomarker_entity.recommended_name
            - biomarker_component.assessed_biomarker_entity_id
            - biomarker_component.assessed_entity_type
            - condition.id
            - condition.recommended_name.id
            - condition.recommended_name.name
            - condition.recommended_name.resource
        - These fields can be merged:
            - biomarker_component.assessed_biomarker_entity.synonyms
            - biomarker_component.specimen
            - biomarker_component.evidence_source
            - best_biomarker_role
            - condition.synonyms
            - evidence_source
            - citation

    Parameters
    ----------
    merge_record: dict
        The record to merge into.
    collision_record: dict
        The record to merge into the merge record.

    Returns
    -------
    dict or None
        If the merge is successful, the merged record is returned. If the record can't be merged,
        None is returned.
    """
    if merge_record["biomarker_id"] != collision_record["biomarker_id"]:
        raise ValueError(
            "Attempting merge on mismatched biomarker id records",
            f"Merge record id: {merge_record['biomarker_id']}",
            f"Collision record id: {collision_record['biomarker_id']}",
        )
    if (
        merge_record["biomarker_canonical_id"]
        != collision_record["biomarker_canonical_id"]
    ):
        raise ValueError(
            "Attempting merge on mismatched biomarker canonical id records",
            f"Merge record id: {merge_record['biomarker_canonical_id']}",
            f"Collision record id: {collision_record['biomarker_canonical_id']}",
        )

    merge_record["biomarker_component"].sort(
        key=lambda x: x["assessed_biomarker_entity_id"]
    )
    collision_record["biomarker_component"].sort(
        key=lambda x: x["assessed_biomarker_entity_id"]
    )

    if _check_merge_fail_conditions(
        merge_record=merge_record, collision_record=collision_record
    ):
        return None

    return _merge_records(merge_record, collision_record)


def _merge_records(merge_record: dict, collision_record: dict) -> dict:
    """Performs the merging of the two records.

    Returns
    -------
    dict
        The merged record.
    """
    for merge_comp, collision_comp in zip(
        merge_record["biomarker_component"], collision_record["biomarker_component"]
    ):
        merge_comp["assessed_biomarker_entity"]["synonyms"].extend(
            synonym
            for synonym in collision_comp["assessed_biomarker_entity"]["synonyms"]
            if synonym not in merge_comp["assessed_biomarker_entity"]["synonyms"]
        )

        merge_comp["specimen"].extend(
            specimen
            for specimen in collision_comp["specimen"]
            if specimen not in merge_comp["specimen"]
        )

        merge_comp["evidence_source"] = _merge_evidence_sources(
            merge_comp["evidence_source"], collision_comp["evidence_source"]
        )

    merge_record["condition"]["synonyms"].extend(
        synonym
        for synonym in collision_record["condition"]["synonyms"]
        if synonym not in merge_record["condition"]["synonyms"]
    )

    merge_record["best_biomarker_role"].extend(
        role
        for role in collision_record["best_biomarker_role"]
        if role not in merge_record["best_biomarker_role"]
    )

    merge_record["evidence_source"] = _merge_evidence_sources(
        merge_record["evidence_source"], collision_record["evidence_source"]
    )

    merge_record["citation"].extend(
        citation
        for citation in collision_record["citation"]
        if citation not in merge_record["citation"]
    )

    return merge_record


def _check_merge_fail_conditions(merge_record: dict, collision_record: dict) -> bool:
    """Check whether any of the merge failure conditions are met.

    Returns
    -------
    bool
        True if the records cannot be merged, False if none of the failure conditions are met.
    """
    merge_biomarker_component: list = merge_record["biomarker_component"]
    collision_biomarker_component: list = collision_record["biomarker_component"]

    if len(merge_biomarker_component) != len(collision_biomarker_component):
        return True

    for merge_comp, collision_comp in zip(
        merge_biomarker_component, collision_biomarker_component
    ):
        if (
            merge_comp["biomarker"] != collision_comp["biomarker"]
            or merge_comp["assessed_biomarker_entity"]["recommended_name"]
            != merge_comp["assessed_biomarker_entity"]["recommended_name"]
            or merge_comp["assessed_biomarker_entity_id"]
            != collision_comp["assessed_biomarker_entity_id"]
            or merge_comp["assessed_entity_type"]
            != collision_comp["assessed_entity_type"]
        ):
            return True

    if (
        merge_record["condition"]["id"] != collision_record["condition"]["id"]
        or merge_record["condition"]["recommended_name"]["id"]
        != collision_record["condition"]["recommended_name"]["id"]
        or merge_record["condition"]["recommended_name"]["name"]
        != merge_record["condition"]["recommended_name"]["name"]
        or merge_record["condition"]["recommended_name"]["resource"]
        != collision_record["condition"]["recommended_name"]["resource"]
    ):
        return True

    return False


def _merge_evidence_sources(merge_sources: list, collision_sources: list) -> list:
    """Merges evidence source lists."""
    for collision_source in collision_sources:
        existing_source = next(
            (
                source
                for source in merge_sources
                if source["id"] == collision_source["id"]
                and source["database"] == collision_source["database"]
                and source["url"] == collision_source["url"]
            ),
            None,
        )

        if existing_source:
            existing_source["evidence_list"].extend(
                evidence
                for evidence in collision_source["evidence_list"]
                if evidence not in existing_source["evidence_list"]
            )

            existing_source["tags"].extend(
                tag
                for tag in collision_source["tags"]
                if tag not in existing_source["tags"]
            )
        else:
            merge_sources.append(collision_source)

    return merge_sources


def is_dir_empty(path: str) -> bool:
    """Checks whether a directory exists and is empty."""
    return os.path.isdir(path) and len(os.listdir(path)) == 0
