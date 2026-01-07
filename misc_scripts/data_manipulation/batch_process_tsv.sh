#!/bin/bash
# Wrapper script for batch processing multiple TSV files with cached vocabularies

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/append_biomarker_vocab.py"

# Configuration
JSON_DIR="/data/shared/biomarkerdb/generated/datamodel/merged/current/merged_json_updated"
CACHE_FILE="$SCRIPT_DIR/biomarker_vocab_cache.pkl"
DOWNLOADS_BASE="/data/shared/biomarkerdb/downloads"

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS] <tsv_file1> [tsv_file2] ...
   or: $0 --from-config <config_file>

Batch process TSV files to append biomarker_controlled_vocab column.

OPTIONS:
    -h, --help              Show this help message
    -c, --create-cache      Create cache file from JSON directory
    -j, --json-dir DIR      JSON directory (default: $JSON_DIR)
    --cache-file FILE       Cache file path (default: $CACHE_FILE)
    --no-cache              Don't use cache, load from JSON each time
    --from-config FILE      Process TSV files from directories listed in config file
    --downloads-base DIR    Base path for downloads (default: $DOWNLOADS_BASE)

CONFIG FILE FORMAT:
    Text file with one directory name per line. The script will process all
    *.tsv files in \$DOWNLOADS_BASE/<dir_name>/current/
    
    Example config file:
        oncomx
        cptac
        pride

EXAMPLES:
    # Create cache file (do this once)
    $0 --create-cache

    # Process multiple TSV files using cache
    $0 file1.tsv file2.tsv file3.tsv

    # Process TSV files from directories in config file
    $0 --from-config directories.txt

    # Process from config with custom base path
    $0 --from-config dirs.txt --downloads-base /other/path

    # Process files from a different JSON directory
    $0 --json-dir /other/path file1.tsv

    # Process without cache (slower)
    $0 --no-cache file1.tsv

    # Process all TSV files in a directory
    $0 /path/to/tsv/*.tsv

EOF
    exit 1
}

# Function to process TSV files from config
process_from_config() {
    local config_file="$1"
    
    if [ ! -f "$config_file" ]; then
        echo "Error: Config file not found: $config_file" >&2
        exit 1
    fi
    
    echo "Reading directories from config file: $config_file" >&2
    
    # Read config file and collect all TSV files
    local all_tsv_files=()
    local line_num=0
    
    while IFS= read -r dir_name || [ -n "$dir_name" ]; do
        line_num=$((line_num + 1))
        
        # Skip empty lines and comments
        [[ -z "$dir_name" || "$dir_name" =~ ^[[:space:]]*# ]] && continue
        
        # Trim whitespace
        dir_name=$(echo "$dir_name" | xargs)
        
        local tsv_path="$DOWNLOADS_BASE/$dir_name/current"
        
        echo "Processing directory: $dir_name" >&2
        echo "  Looking in: $tsv_path" >&2
        
        if [ ! -d "$tsv_path" ]; then
            echo "  Warning: Directory not found, skipping" >&2
            continue
        fi
        
        # Find all TSV files in the directory
        local found_files=()
        if compgen -G "$tsv_path/*.tsv" > /dev/null; then
            for file in "$tsv_path"/*.tsv; do
                [ -f "$file" ] && found_files+=("$file")
            done
        fi
        
        if [ ${#found_files[@]} -eq 0 ]; then
            echo "  Warning: No TSV files found in $tsv_path" >&2
        else
            echo "  Found ${#found_files[@]} TSV file(s)" >&2
            all_tsv_files+=("${found_files[@]}")
        fi
        
    done < "$config_file"
    
    if [ ${#all_tsv_files[@]} -eq 0 ]; then
        echo "Error: No TSV files found in any of the configured directories" >&2
        exit 1
    fi
    
    echo "" >&2
    echo "Total TSV files to process: ${#all_tsv_files[@]}" >&2
    echo "" >&2
    
    # Return the array of TSV files
    printf '%s\n' "${all_tsv_files[@]}"
}

# Parse arguments
CREATE_CACHE=false
USE_CACHE=true
CONFIG_FILE=""
TSV_FILES=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -c|--create-cache)
            CREATE_CACHE=true
            shift
            ;;
        -j|--json-dir)
            JSON_DIR="$2"
            shift 2
            ;;
        --cache-file)
            CACHE_FILE="$2"
            shift 2
            ;;
        --no-cache)
            USE_CACHE=false
            shift
            ;;
        --from-config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --downloads-base)
            DOWNLOADS_BASE="$2"
            shift 2
            ;;
        *)
            TSV_FILES+=("$1")
            shift
            ;;
    esac
done

# Create cache if requested
if [ "$CREATE_CACHE" = true ]; then
    echo "Creating cache file from $JSON_DIR..."
    python3 "$PYTHON_SCRIPT" --create-cache "$JSON_DIR" --cache-file "$CACHE_FILE"
    echo "Cache created successfully at $CACHE_FILE"
    exit 0
fi

# Process from config file if specified
if [ -n "$CONFIG_FILE" ]; then
    echo "Processing TSV files from config: $CONFIG_FILE"
    mapfile -t TSV_FILES < <(process_from_config "$CONFIG_FILE")
fi

# Check if TSV files were provided or found
if [ ${#TSV_FILES[@]} -eq 0 ]; then
    echo "Error: No TSV files specified"
    usage
fi

# Process TSV files
if [ "$USE_CACHE" = true ]; then
    # Check if cache exists
    if [ ! -f "$CACHE_FILE" ]; then
        echo "Cache file not found at $CACHE_FILE"
        echo "Creating cache file..."
        python3 "$PYTHON_SCRIPT" --create-cache "$JSON_DIR" --cache-file "$CACHE_FILE"
    fi
    
    echo "Processing ${#TSV_FILES[@]} TSV file(s) using cache..."
    python3 "$PYTHON_SCRIPT" "${TSV_FILES[@]}" --cache-file "$CACHE_FILE"
else
    echo "Processing ${#TSV_FILES[@]} TSV file(s) without cache..."
    for tsv_file in "${TSV_FILES[@]}"; do
        python3 "$PYTHON_SCRIPT" "$tsv_file" --json-dir "$JSON_DIR"
    done
fi

echo "All files processed successfully!"
