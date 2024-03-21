# Biomarker ID Assignment Process 

This guide walks you through how to assign new, incoming data to their corresponding biomarker ID's, how to load the processed data into the MongoDB instance, and how to prepare the data release version for the JSON data model formatted data.

- [Assign Biomarker IDs](#assign-biomarker-ids)
- [Populate the Database](#populate-the-database)
- [Copy Files](#copy-files)

Prerequisites: Make sure to activate (and build) the python virtual environment before running the ID process. For instructions, you can refer to [this guide](https://github.com/clinical-biomarkers/biomarker-partnership/blob/main/supplementary_files/documentation/virtual_env.md).

## Assign Biomarker IDs 

To assign biomarker IDs to your new data, run the `id_assign.py` script from the `/id` directory. This script can only be run from the `tst` server. More information about the under the hood implementation of the ID generation is available in the [ID Implementation Documentation](/docs/id_implementation.md) readme. 

While processing each data record, each data record will be assigned its corresponding `biomarker_canonical_id`. Once the aggregate canonical ID is assigned, the record will be assigned a second level identifier. Whether a collision is found or not, the record will be assigned an additional key called `collision`. This key will have a value of `0` indicating no collision or `1` indicating a collision. If a value of `1` is assigned, some additional information wil lbe added to that specific source file's collision report (which is saved into the `id/collision_reports` subdirectory). This key will be used during the data load process and subsequently removed before loading the data. This value determines which MongoDB collection the data record will be loaded into. 

```bash 
cd id
python id_assign.py -s $SER
```

## Populate the Database

To load the processed data, run the `load_data.py` script from the `/id` directory. You have to complete the ID assignment steps before this step can be completed. The data should be in the filepath `/data/shared/biomarkerdb/generated/datamodel/new_data/current` where `current/` is a symlink to the current version directory. 

Optionally, you can also create a load map that indicates if any files should be completely loaded into the `unreviewed` MongoDB collection (bypassing/overriding any checks on the `collision` key). If included, this file should be called `load_map.json` and should be placed in the same directory as the data. The format of the file allows you to specify which files should be loaded where. The format can look like: 

```json
{
    "unreviewed": ["file_1.json", "file_2.json", ..., "file_n.json"],
    "reviewed": ["file_1.json", "file_2.json", ..., "file_n.json"]
}
```

If you only include one of keys, then the rest of the files will be assumed to be loaded into the other collection. For example, if you are attempting to load 3 files called `file_1.json`, `file_2.json`, and `file_3.json` we can specify in the load map:

```json
{
    "unreviewed": ["file_1.json"]
}
```

In this case, `file_1.json` will be loaded into the unreviewed collection and the other two files will be loaded into the reviewed collection. Alternatively, you can specify:

```json
{
    "reviewed": ["file_2.json", "file_3.json"]
}
```

This will have the same result of the above example. You can also explicitly list both the `unreviewed` and `reviewed` keys and list out all of the files but that can become quite verbose in large data releases. To prevent some errors, the `load_data.py` script will prompt the user for confirmation of their choices before continuing to the data load.

```bash 
python load_data.py -s $SER
```

Where the `$SER` argument is the specified server. 

The code will do some preliminary checks on the data that is to be loaded. It will make sure that each record has a valid formatted biomarker ID.

## Copy Files 

After the data has been properly ID assigned, collisions have been handled, and the `tst` and `prd` databases have been loaded, run the `copy_files.py` script to copy the files into the `existing_data` directory. This is the master directory which holds all the data files that have been loaded into the backend API over the history of the project. This must be run from the `tst` server.

```bash 
python copy_files.py -s tst
```

After all these steps have been completed, the data has been successfully assigned their unique IDs and prepared for a new data release.
