# Biomarker ID Endpoint

### Workflow

1. Query the `biomarker_collection` for the record with the matching `biomarker_id`.
  - The `biomarker_collection` has a unique index on the `biomarker_id` field so there will only be at most 1 returned record.
2. Handle optional `paginated_tables` parameters.
  - This will be handled by the `get_paginated_sections` function (not a general util function).
3. The processed citation data is overwritten over the existing (raw) citation data.
4. The JSON is returned.

### `get_paginated_sections`

Example query string parameters: 
```json
{
  "paginated_tables": [
    {
      "table_id": "citation",
      "offset": 1,
      "limit": 200,
      "sort": "date",
      "order": "desc"
    }
  ]
}
```

- Only `table_id` supported is the `citation` field. 
- Depending on the parameters sent by the user the biomarker record is sorted according to the `order` value (can be `asc` or `desc`) relative to the `sort` value (which can be `Date`, `Title`, `Author`, `Jounral`).
- The citation results can also filtered by the `limit` value and retrived by the `offset` value.
