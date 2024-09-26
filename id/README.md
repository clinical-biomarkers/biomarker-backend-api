# Biomarker ID Assignment Process

This guide walks you through how to assign new incoming data to their corresponding biomarker ID's.

- [Directory Structure](#directory-structure)
- [Assign Biomarker IDs](#assign-biomarker-ids)
- [Copy Files](#copy-files)

Prerequisites: Make sure to activate (and build) the python virtual environment before running the ID process. For instructions, you can refer to [this guide](https://github.com/clinical-biomarkers/biomarker-partnership/blob/main/supplementary_files/documentation/virtual_env.md).

## Directory Structure

| Directory/File        | Description                                                                               |
| --------------------- | ----------------------------------------------------------------------------------------- |
| `helpers/`            | Helper module for the ID assignment process.                                              |
| `check_unique_ids.py` | Script that checks an ID assigned file for potential duplicate IDs. Useful for debugging. |
| `id_assign.py`        | Entry point for assigning biomarker IDs to data files.                                    |
| `copy_files.py`       | Copies the latest files to the `existing_data` directory.                                 |

## Assign Biomarker IDs

To assign biomarker IDs to your new data, run the `id_assign.py` script from the `/id` directory. This script can only be run from the `tst` server. More information about the under the hood implementation of the ID generation is available in the [ID Implementation Documentation](/docs/id_implementation.md).

While processing each data record, each data record will be assigned its corresponding `biomarker_canonical_id`. Once the aggregate canonical ID is assigned, the record will be assigned a second level identifier. Whether a collision is found or not, the record will be assigned an additional key called `collision`. This key will have a value of `0` indicating no collision, `1` indicating a standard collision, or `2` indicating a hard collision. If a value of `1` is assigned, some additional information will be added to that specific source file's collision report (which is saved into the `id/collision_reports` subdirectory). This key will be used during the data preprocessing and subsequently removed before loading the data. This value determines which MongoDB collection the data record will be loaded into.

```bash
cd id
python id_assign.py -s $SER
```

## Copy Files

After the data has been properly ID assigned, run the `copy_files.py` script to copy the files into the `existing_data` directory. This is the master directory which holds all the most recent data files. This must be run from the `tst` server.

```bash
python copy_files.py -s tst
```

After all these steps have been completed, the data has been successfully assigned their unique IDs and prepared for a new data release.
