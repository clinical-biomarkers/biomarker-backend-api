import json

# Open and read mondo.json
with open("mondo.json", "r") as f:
    mondo = json.load(f)

# Loop through the ontology classes and identify anything that has a DOID xref
curr_json_entry = ""
if not curr_json_entry.get("meta", {}).get("deprecated", false):
    continue
# In the output file, the top level keys are DOIDs
# The values are: 1) lbl 2) ID extracted from the URL

# Output a JSON file
with open("mondo_out.json", "w") as outfile:
    json.dump(dictionary, outfile)
