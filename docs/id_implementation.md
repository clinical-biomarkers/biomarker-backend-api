# ID Backend Implementation Details

- [Background](#background)
  - [Data Model Structure](#data-model-structure)
  - [ID Structure](#id-structure)
    - [Canonical ID](#canonical-id-biomarkercanonicalid)
    - [Second Level ID](#second-level-id-biomarkerid)
- [Implementation](#backend-implementation)
  - [MongoDB Collections](#mongodb-collections)
    - [Canonical ID Map Collection](#canonical-id-biomarkercanonicalid)
    - [Second Level ID Map Collection](#second-level-id-map-collection)

# Background

## Data Model Structure

```json
{
  "biomarker_canonical_id": "",
  "biomarker_id": "",
  "biomarker_component": [
    {
      "biomarker": "",
      "assessed_biomarker_entity": {
        "recommended_name": "",
        "synonyms": [
          {
            "synonym": ""
          }
        ]
      },
      "assessed_biomarker_entity_id": "",
      "assessed_entity_type": "",
      "specimen": [
        {
          "name": "",
          "specimen_id": "",
          "name_space": "",
          "url": "",
          "loinc_code": ""
        }
      ],
      "evidence_source": [
        {
          "evidence_id": "",
          "database": "",
          "url": "",
          "evidence_list": [
            {
              "evidence": ""
            }
          ],
          "tags": [
            {
              "tag": ""
            }
          ]
        }
      ]
    }
  ],
  "best_biomarker_role": [
    {
      "role": ""
    }
  ],
  "condition": {
    "condition_id": "",
    "recommended_name": {
      "condition_id": "",
      "name": "",
      "description": "",
      "resource": "",
      "url": ""
    },
    "synonyms": [
      {
        "synonym_id": "",
        "name": "",
        "resource": "",
        "url": ""
      }
    ]
  },
  "exposure_agent": {
    "exposure_agent_id": "",
    "recommended_name": {
      "exposure_agent_id": "",
      "name": "",
      "description": "",
      "resource": "",
      "url": ""
    }
  },
  "evidence_source": [
    {
      "evidence_id": "",
      "database": "",
      "url": "",
      "evidence_list": [
        {
          "evidence": ""
        }
      ],
      "tags": [
        {
          "tag": ""
        }
      ]
    }
  ],
  "citation": [
    {
      "citation_title": "",
      "journal": "",
      "authors": "",
      "date": "",
      "reference": [
        {
          "reference_id": "",
          "type": "",
          "url": ""
        }
      ],
      "evidence_source": {
        "evidence_id": "",
        "database": "",
        "url": ""
      }
    }
  ]
}
```

## ID Structure

### Canonical ID (biomarker_canonical_id)

- The canonical ID is based on the `biomarker` and `assessed_biomarker_entity` fields.
- A unique pair of `biomarker` and `assessed_biomarker_entity` will be assigned a new `biomarker_canonical_id`.

### Second Level ID (biomarker_id)

- The second level ID is based upon the combination of the `biomarker_canonical_id` and `condition_id` fields.

# Backend Implementation

- When processing data for ID assignment, the `biomarker` and `assessed_biomarker_entity` fields will be normalized, sorted, and concatenated.
- The resulting string will be hashed and compared against our existing ID collection.
- If no collision is found:
  - The new hash value will be added to the canonical ID map collection and the record will be assigned a new `biomarker_canonical_id`.
  - The second level `biomarker_id` will be assigned with a value in the format of `{biomarker_canonical_id}-1`.
    - The second level ID will be added to the second level ID map collection.
- If a collision is found:
  - The record will be assigned the existing `biomarker_canonical_id` that caused the collision.
  - The second level ID map collection will be queried on the `biomarker_canonical_id` and the existing_entries (representing the existing condition pairs that already exist under that canonical ID) will be checked for existence of that condition value already.
    - If no collision is found:
      - The current index will be incremented and the `n + 1` value will be assigned in the format of `{biomarker_canonical_id}-{n + 1}`.
      - The new entry will be added to the second level ID map collection.
    - If a collision is found:
      - The data record will be marked as a collision to be loaded into the collision data collection.

## MongoDB Collections

### Canonical ID Map Collection

```json
{
	"hash_value": <VALUE>,
	"biomarker_canonical_id": <VALUE>, // example: AA0001
	"core_values_str": <VALUE> // the string that was used to create the hash value
}
```

- There is a unique field index on the `hash_value` key.

### Second Level ID Map Collection

```json
{
	"biomarker_canonical_id": "<VALUE>",
	"values": {
		"cur_index": "{n}", // example: 2
		"existing_entries": [
			{
				"{condition_id}": "{biomarker_canonical_id}-1"
			},
			...,
			{
				"{condition_id}": "{biomarker_canonical_id}-n"
			}
		]
	}
}
```
- There is a unique field index on the `biomarker_canonical_id` key.
