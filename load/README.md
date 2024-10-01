# Data Preprocessing and Loading

## Preprocessing

The `preprocess.py` script will perform two passes on the data.

During the first pass:

- The files found in the existing data directory are looped through.
- Each file is streamed using the ijson library.
- If the collision value is `0`, that specific record will be saved in a file in the merged directory with the filename `{biomarker_id}.json`.
- If the collision value is `1`, that record will be saved in a file in the collision_dir with the filename `{biomarker_id}-{counter}.json`.
- If the collision value is `2`, that record will be skipped.

During the second pass:

- Glob all the JSON files in the collision directory.
- Compare to existing record in the merged directory.
- Check if it can be automatically merged.
- If it can be merged, merge it into the existing record and delete the record in the collision directory.
  - If merged, will have to re-update any other collision filename counters.
- If it can't be merged, leave it in the collision directory.

### Merge Criteria

- Changes in these fields cause merge failures:
  - `biomarker_component.biomarker`
  - `biomarker_component.assessed_biomarker_entity.recommended_name`
  - `biomarker_component.assessed_biomarker_entity_id`
  - `biomarker_component.assessed_entity_type`
  - `condition.id`
  - `condition.recommended_name.id`
  - `condition.recommended_name.name`
  - `condition.recommended_name.resource`

These fields can be merged:  
 - `biomarker_component.assessed_biomarker_entity.synonyms`  
 - `biomarker_component.specimen`  
 - `biomarker_component.evidence_source`  
 - `best_biomarker_role`  
 - `condition.synonyms`  
 - `evidence_source`  
 - `citation`

## Loading

The `load_data.py` script will load all the files in the merged directory into the biomarker collection. All the files remaining in the collision directory will be loaded into the unreviewed collection.
