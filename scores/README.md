# Score Mapping

The `map_scores.py` script allows you to take score information outputted by the [biomarker-score-calculator](https://github.com/clinical-biomarkers/biomarker-score-calculator) and attach them to biomarker recrods. The biomarker score calculator CLI tool outputs a JSON file with the files, biomarker IDs, biomarker scores, and score breakdowns. This file is read in by the `map_scores.py` script and mapped to the existing source files.
