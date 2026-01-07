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
                
            # Extract biomarker_controlled_vocab from first biomarker_component
            if 'biomarker_component' in data and len(data['biomarker_component']) > 0:
                vocab = data['biomarker_component'][0].get('biomarker_controlled_vocab', '')
                vocab_map[biomarker_id] = vocab
            else:
                vocab_map[biomarker_id] = ''
                
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
    
    Handles:
    - Removing duplicate biomarker_controlled_vocab columns
    - Adding column if missing
    - Validating increased/decreased consistency
    """
    # Read the TSV file
    print(f"Processing TSV file: {tsv_file}")
    rows = []
    issues_found = []
    
    with open(tsv_file, 'r', newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        fieldnames = list(reader.fieldnames)
        
        if 'biomarker_id' not in fieldnames:
            print("Error: 'biomarker_id' column not found in TSV file", file=sys.stderr)
            sys.exit(1)
        
        # Check for duplicate biomarker_controlled_vocab columns
        vocab_col_count = fieldnames.count('biomarker_controlled_vocab')
        
        if vocab_col_count > 1:
            print(f"Warning: Found {vocab_col_count} 'biomarker_controlled_vocab' columns. Removing duplicates...")
            # Keep only the first occurrence
            new_fieldnames = []
            seen_vocab = False
            for field in fieldnames:
                if field == 'biomarker_controlled_vocab':
                    if not seen_vocab:
                        new_fieldnames.append(field)
                        seen_vocab = True
                    # Skip duplicate columns
                else:
                    new_fieldnames.append(field)
            fieldnames = new_fieldnames
        elif vocab_col_count == 0:
            print("Info: 'biomarker_controlled_vocab' column not found. Adding it...")
            fieldnames.append('biomarker_controlled_vocab')
        
        # Process each row
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            biomarker_id = row.get('biomarker_id', '')
            biomarker = row.get('biomarker', '')
            
            # Look up controlled vocab from JSON
            controlled_vocab_from_json = vocab_map.get(biomarker_id, '')
            
            # Get existing value if present
            existing_vocab = row.get('biomarker_controlled_vocab', '')
            
            # Prefer JSON lookup if available, otherwise keep existing
            if controlled_vocab_from_json:
                controlled_vocab = controlled_vocab_from_json
            else:
                controlled_vocab = existing_vocab
            
            row['biomarker_controlled_vocab'] = controlled_vocab
            
            # Validate increased/decreased consistency
            if biomarker and controlled_vocab:
                biomarker_lower = biomarker.lower()
                
                # Check if biomarker contains "increased" or "decreased"
                has_increased = 'increased' in biomarker_lower
                has_decreased = 'decreased' in biomarker_lower
                
                if has_increased or has_decreased:
                    vocab_starts_increased = controlled_vocab.startswith('Increased')
                    vocab_starts_decreased = controlled_vocab.startswith('Decreased')
                    
                    # Validate consistency
                    is_consistent = True
                    if has_increased and not vocab_starts_increased:
                        is_consistent = False
                        issues_found.append({
                            'row': row_num,
                            'biomarker_id': biomarker_id,
                            'biomarker': biomarker,
                            'controlled_vocab': controlled_vocab,
                            'issue': 'Biomarker contains "increased" but controlled_vocab does not start with "Increased"'
                        })
                    elif has_decreased and not vocab_starts_decreased:
                        is_consistent = False
                        issues_found.append({
                            'row': row_num,
                            'biomarker_id': biomarker_id,
                            'biomarker': biomarker,
                            'controlled_vocab': controlled_vocab,
                            'issue': 'Biomarker contains "decreased" but controlled_vocab does not start with "Decreased"'
                        })
            
            # Keep only the fields we want (removes duplicate columns)
            cleaned_row = {field: row.get(field, '') for field in fieldnames}
            rows.append(cleaned_row)
    
    # Write the output
    output_path = output_file if output_file else tsv_file
    print(f"Writing output to: {output_path}")
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Successfully processed {len(rows)} rows")
    
    # Report issues if found
    if issues_found:
        print(f"\n⚠️  Warning: Found {len(issues_found)} increased/decreased consistency issues:")
        for issue in issues_found[:10]:  # Show first 10 issues
            print(f"  Row {issue['row']}: {issue['biomarker_id']}")
            print(f"    Biomarker: {issue['biomarker']}")
            print(f"    Controlled vocab: {issue['controlled_vocab']}")
            print(f"    Issue: {issue['issue']}")
        
        if len(issues_found) > 10:
            print(f"  ... and {len(issues_found) - 10} more issues")
        
        # Save detailed report
        report_file = output_path.replace('.tsv', '_issues.txt')
        with open(report_file, 'w') as f:
            f.write(f"Increased/Decreased Consistency Issues Report\n")
            f.write(f"File: {tsv_file}\n")
            f.write(f"Total issues: {len(issues_found)}\n")
            f.write(f"="*80 + "\n\n")
            for issue in issues_found:
                f.write(f"Row {issue['row']}: {issue['biomarker_id']}\n")
                f.write(f"  Biomarker: {issue['biomarker']}\n")
                f.write(f"  Controlled vocab: {issue['controlled_vocab']}\n")
                f.write(f"  Issue: {issue['issue']}\n\n")
        print(f"\n📄 Detailed report saved to: {report_file}")
    else:
        print("✓ No increased/decreased consistency issues found")


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
