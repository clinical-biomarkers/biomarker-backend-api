# Miscellaneous Scripts

Some miscellaneous useful scripts.

- [Directory Structure](#directory-structure)
- [Create Concat Field](#createconcatfieldpy)
- [Peak Collections](#peakcollectionpy)
- [Check Indexes](#checkindexespy)

## Directory Structure

| Directory/File           | Description                                               |
| ------------------------ | --------------------------------------------------------- |
| `check_collections.py`   | Prints the collections in the database.                   |
| `create_concat_field.py` | Creates the `all_text` internal field for the text index. |
| `check_indexes.py`       | Check the indexes on a specified collection.              |
| `peak_collection.py`     | Peak at the recent entries in a specified collection.     |

## check_collection.py

Prints the collections in the database.

## create_concat_field.py

Creates the `all_text` internal field in the biomarker collection used for the text index. The `all_text` field is a concatenated string of most of the string fields in the biomarker data model. This allows us to index the `all_text` field directly, avoiding a more costly wildcard text index.

## peak_collection.py

Let's you peak into one of the collections in the database. When running the script, include the flag for the collection you want to peak into. You can only peak one collection at a time. You can also optionally pass in the `-n` argument to specify how many entries you want to view (by default the last 5 entries entered into the collection are returned).

Collection flags are as follows:

| Flag | Collection                 |
| ---- | -------------------------- |
| `-b` | biomarker collection       |
| `-m` | canonical ID collection    |
| `-s` | second level ID collection |
| `-e` | error collection           |
| `-c` | cache collection           |

## check_indexes.py

Let's you check which indexes are currently setup in the target collection. Collection flags are as specified in the `peak_collection.py` file.
