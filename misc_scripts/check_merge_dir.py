import sys
import os
import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.config import get_config
from tutils.general import load_json_type_safe


def main() -> None:

    config_obj = get_config()
    data_path = config_obj["data_path"]
    generated_path_segment = config_obj["generated_path_segment"]
    merged_data_segment = config_obj["merged_data_segment"]

    merged_dir = os.path.join(
        data_path, *generated_path_segment, *merged_data_segment, "merged_json"
    )
    glob_pattern = os.path.join(merged_dir, "*.json")

    files = glob.glob(glob_pattern)
    basenames = {os.path.splitext(os.path.basename(x))[0] for x in files}

    if len(files) != len(basenames):
        print(
            f"Mismatch files ({len(files)}) and base names ({len(basenames)}) lengths."
        )

    seen_ids: set[str] = set()
    for file in files:
        data = load_json_type_safe(filepath=file, return_type="dict")
        biomarker_id = data["biomarker_id"]
        if biomarker_id in seen_ids:
            print(f"Duplicate ID `{biomarker_id}` from {file}.")
        else:
            seen_ids.add(biomarker_id)


if __name__ == "__main__":
    main()
