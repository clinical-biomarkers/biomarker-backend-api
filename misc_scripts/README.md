# Miscellaneous Scripts

Some useful scripts for debugging.

## peak_collection.py

Let's you peak into one of the collections in the database. Currently is only setup for the `tst` server. When running the script, include the flag for the collection you want to peak into. You can only peak one collection at a time. You can also optionally pass in the `-n` argument to specify how many entries you want to view (by default the last 5 entries entered into the collection are returned).

Collection flags are as follows:

| Flag | Collection                 |
| ---- | -------------------------- |
| `-b` | biomarker collection       |
| `-m` | canonical ID collection    |
| `-s` | second level ID collection |
| `-e` | error collection           |
| `-c` | cache collection           |

## check_indexes.py

Let's you check which indexes are currently setup in the target collection. Collection flags are as specified in the `peak_collection.py` file. Currently is only setup for the `tst` server.
