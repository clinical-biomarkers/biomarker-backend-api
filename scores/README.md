# Biomarker Scores

Scripts for mapping and updating bimoarker scores calculated from the [biomarker-score-calculator](https://github.com/clinical-biomarkers/biomarker-score-calculator).

- [Directory Structure](#directory-structure)
- [Map Scores to Data](#mapscorespy)
- [Update Scores in Database](#updatescorespy)

## Directory Structure

| Directory/File     | Description                                                                         |
| ------------------ | ----------------------------------------------------------------------------------- |
| `map_scores.py`    | Maps scores from the score output file from the score calculator to the data files. |
| `update_scores.py` | Updates the score fields in MongoDB.                                                |

## map_scores.py

The `map_scores.py` script allows you to take score information outputted by the [biomarker-score-calculator](https://github.com/clinical-biomarkers/biomarker-score-calculator) and attach them to biomarker recrods. The biomarker score calculator CLI tool outputs a JSON file with the files, biomarker IDs, biomarker scores, and score breakdowns. This file is read in by the `map_scores.py` script and mapped to the existing source files.

**Note**: When entering the glob pattern argument for the script it must be wrapped in quotation marks, otherwise the pattern will be unpacked directly in the command line causing the program to get many filepaths as arguments. For example, if you enter `/data/shared/biomarkerdb/generated/datamodel/existing/data/*.json` as the glob pattern argument it will be directly unpacked causing every file captured by that glob pattern to be a separate argument. Instead, enter `"/data/shared/biomarkerdb/generated/datamodel/existing/data/*.json"` as the argument.

## update_scores.py

The `update_scores.py` script allows you to update the score values in MongoDB. Takes the server (`tst` or `prd`) and a glob pattern as arguments (refer to the `Note` in the `map_scores.py` section about the glob pattern argument).
