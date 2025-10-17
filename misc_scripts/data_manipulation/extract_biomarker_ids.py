import json
import glob
import os
from pathlib import Path

def extract_biomarker_ids(input_pattern="/data/shared/biomarkerdb/generated/datamodel/existing_data/*.json", output_file="/home/maria.kim/test_playground/canonical_ids.json"):
    """
    Extract biomarker_id values from multiple JSON files and save to output file.
    
    Args:
        input_pattern (str): Glob pattern to match input JSON files (default: "*.json")
        output_file (str): Name of output JSON file (default: "biomarker_ids.json")
    """
    biomarker_ids = []
    
    # Find all JSON files matching the pattern
    json_files = glob.glob(input_pattern)
    
    if not json_files:
        print(f"No JSON files found matching pattern: {input_pattern}")
        return
    
    print(f"Found {len(json_files)} JSON files to process...")
    
    # Process each JSON file
    for file_path in json_files:
        try:
            print(f"Processing: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Handle both list of objects and single object
            if isinstance(data, list):
                # Extract biomarker_id from each object in the list
                for item in data:
                    if isinstance(item, dict) and "biomarker_canonical_id" in item:
                        biomarker_ids.append(item["biomarker_canonical_id"])
            elif isinstance(data, dict) and "biomarker_canonical_id" in data:
                # Single object with biomarker_id
                biomarker_ids.append(data["biomarker_canonical_id"])
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON file {file_path}: {e}")
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
    
    # Remove duplicates while preserving order
    unique_biomarker_ids = list(dict.fromkeys(biomarker_ids))
    
    # Save extracted IDs to output file
    try:
        with open(output_file, 'w', encoding='utf-8') as output:
            json.dump(unique_biomarker_ids, output, indent=2)
        
        print(f"\nExtraction complete!")
        print(f"Total biomarker IDs found: {len(biomarker_ids)}")
        print(f"Unique biomarker IDs: {len(unique_biomarker_ids)}")
        print(f"Output saved to: {output_file}")
        
    except Exception as e:
        print(f"Error writing output file {output_file}: {e}")

def main():
    """
    Main function with example usage
    """
    # Example 1: Process all JSON files in current directory
    extract_biomarker_ids()
    
    # Example 2: Process JSON files in a specific directory
    # extract_biomarker_ids("data/*.json", "extracted_ids.json")
    
    # Example 3: Process specific files
    # extract_biomarker_ids("biomarker_data_*.json", "biomarker_list.json")

if __name__ == "__main__":
    main()
