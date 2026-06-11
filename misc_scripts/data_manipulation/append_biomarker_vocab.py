#!/usr/bin/env python3
"""
Script to append biomarker_controlled_vocab column to TSV file(s).
Supports batch processing with cached biomarker vocabularies for efficiency.
"""

import json
import csv
import sys
import pickle
from pathlib import Path
import argparse


def load_biomarker_vocab(json_dir):
    """
    Load biomarker_controlled_vocab from all JSON files in directory.
    Returns a dict mapping biomarker_id to biomarker_controlled_vocab.
    """
    vocab_map = {}
    json_path = Path(json_dir)
    
    if not json_path.exists():
        print(f"Error: JSON directory does not exist: {json_dir}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Loading biomarker vocabularies from {json_dir}...")
    json_files = list(json_path.glob("*.json"))
    total = len(json_files)
    
    # Iterate through all JSON files
    for idx, json_file in enumerate(json_files, 1):
        if idx % 1000 == 0:
            print(f"  Processed {idx}/{total} files...", file=sys.stderr)
            
        biomarker_id = json_file.stem  # Filename without .json extension
        
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            # Extract biomarker_controlled_vocab for ALL biomarker_components
            if 'biomarker_component' in data:
                for component in data['biomarker_component']:
                    biomarker_orig = component.get('biomarker_orig', '').lower().strip()
                    vocab = component.get('biomarker_controlled_vocab', '')
                    if biomarker_orig:  # Only add if biomarker field exists
                        key = (biomarker_id, biomarker_orig)
                        vocab_map[key] = vocab
                
        except Exception as e:
            print(f"Warning: Error processing {json_file}: {e}", file=sys.stderr)
            vocab_map[biomarker_id] = ''
    
    print(f"Loaded {len(vocab_map)} biomarker vocabularies")
    return vocab_map


def save_vocab_cache(vocab_map, cache_file):
    """Save vocabulary map to a pickle cache file."""
    print(f"Saving vocabulary cache to {cache_file}...")
    with open(cache_file, 'wb') as f:
        pickle.dump(vocab_map, f)
    print(f"Cache saved successfully")


def load_vocab_cache(cache_file):
    """Load vocabulary map from a pickle cache file."""
    print(f"Loading vocabulary cache from {cache_file}...")
    with open(cache_file, 'rb') as f:
        vocab_map = pickle.load(f)
    print(f"Loaded {len(vocab_map)} biomarker vocabularies from cache")
    return vocab_map


def append_vocab_column(tsv_file, vocab_map, output_file=None):
    """
    Append biomarker_controlled_vocab column to TSV file.
    If output_file is None, overwrites the input file.
    """
    # Read the TSV file
    print(f"Processing TSV file: {tsv_file}")
    rows = []
    with open(tsv_file, 'r', newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        fieldnames = reader.fieldnames
        
        if 'biomarker_id' not in fieldnames:
            print("Error: 'biomarker_id' column not found in TSV file", file=sys.stderr)
            sys.exit(1)

        for row in reader:
            biomarker_id = row['biomarker_id']
            biomarker = row.get('biomarker', '').lower().strip()
            
            # Look up the controlled vocab using (biomarker_id, biomarker) key
            key = (biomarker_id, biomarker)
            vocab = vocab_map.get(key, '')
            row['biomarker_controlled_vocab'] = vocab
            rows.append(row)
    
    # Add the new column to fieldnames
    new_fieldnames = list(fieldnames) + ['biomarker_controlled_vocab']
    
    # Write the output
    output_path = output_file if output_file else tsv_file
    print(f"Writing output to: {output_path}")
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames, delimiter='\t')
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Successfully appended biomarker_controlled_vocab column to {len(rows)} rows")


def main():
    parser = argparse.ArgumentParser(
        description='Append biomarker_controlled_vocab column to TSV file(s)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Process single file (load JSON each time)
  %(prog)s input.tsv --json-dir /path/to/json/

  # Create cache file for reuse
  %(prog)s --create-cache /path/to/json/ --cache-file vocab_cache.pkl

  # Process multiple files using cache
  %(prog)s file1.tsv --cache-file vocab_cache.pkl
  %(prog)s file2.tsv --cache-file vocab_cache.pkl
  %(prog)s file3.tsv --cache-file vocab_cache.pkl

  # Process multiple files in one command using cache
  %(prog)s file1.tsv file2.tsv file3.tsv --cache-file vocab_cache.pkl

  # Process with output files
  %(prog)s input.tsv --json-dir /path/to/json/ --output output.tsv
        '''
    )
    
    parser.add_argument('tsv_files', nargs='*', help='TSV file(s) to process')
    parser.add_argument('--json-dir', help='Directory containing JSON files')
    parser.add_argument('--cache-file', help='Pickle cache file to load/save vocabulary map')
    parser.add_argument('--create-cache', help='Create cache file from JSON directory and exit')
    parser.add_argument('--output', help='Output file (only valid when processing single TSV file)')
    
    args = parser.parse_args()
    
    # Mode 1: Create cache and exit
    if args.create_cache:
        vocab_map = load_biomarker_vocab(args.create_cache)
        if args.cache_file:
            save_vocab_cache(vocab_map, args.cache_file)
        else:
            print("Error: --cache-file must be specified with --create-cache", file=sys.stderr)
            sys.exit(1)
        return
    
    # Mode 2: Process TSV files
    if not args.tsv_files:
        parser.print_help()
        sys.exit(1)
    
    # Load vocabulary map (from cache or JSON directory)
    if args.cache_file:
        vocab_map = load_vocab_cache(args.cache_file)
        # Debug print statements
        # print(f"\nSample keys in vocab_map (first 5):")
        # for i, key in enumerate(list(vocab_map.keys())[:5]):
            # print(f"  {key}")
        # print()
    elif args.json_dir:
        vocab_map = load_biomarker_vocab(args.json_dir)
    else:
        print("Error: Either --cache-file or --json-dir must be specified", file=sys.stderr)
        sys.exit(1)
    
    # Process TSV file(s)
    if len(args.tsv_files) > 1 and args.output:
        print("Error: --output can only be used with a single TSV file", file=sys.stderr)
        sys.exit(1)
    
    for tsv_file in args.tsv_files:
        append_vocab_column(tsv_file, vocab_map, args.output)
        print()


if __name__ == "__main__":
    main()
