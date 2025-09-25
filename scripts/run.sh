#!/bin/bash
set -e

# Default values
DB_PATH="./rocksdb_data"
STRATEGY="page_index"
CLEAN_DATA=false
INITIAL_RECORDS=""
HOTSPOT_UPDATES=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --db-path)
            DB_PATH="$2"
            shift 2
            ;;
        --strategy)
            STRATEGY="$2"
            shift 2
            ;;
        --clean)
            CLEAN_DATA=true
            shift
            ;;
        --initial-records)
            INITIAL_RECORDS="--initial_records $2"
            shift 2
            ;;
        --hotspot-updates)
            HOTSPOT_UPDATES="--hotspot_updates $2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --db-path PATH              Database path (default: ./rocksdb_data)"
            echo "  --strategy STRATEGY          Storage strategy to use (page_index|direct_version, default: page_index)"
            echo "  --clean                     Clean existing data before starting"
            echo "  --initial-records N         Number of initial records"
            echo "  --hotspot-updates N         Number of hotspot updates"
            echo "  --help, -h                  Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Use page_index strategy with default settings"
            echo "  $0 --strategy direct_version         # Use direct_version strategy"
            echo "  $0 --strategy direct_version --clean # Use direct_version and clean data"
            echo "  $0 --initial-records 50000000        # Custom initial records count"
            exit 0
            ;;
        *)
            # If it's not a known option, treat it as database path (backward compatibility)
            if [[ ! $1 =~ ^-- ]]; then
                DB_PATH="$1"
            else
                echo "Error: Unknown option $1"
                echo "Use --help to see available options"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate strategy
if [[ "$STRATEGY" != "page_index" && "$STRATEGY" != "direct_version" ]]; then
    echo "Error: Invalid strategy '$STRATEGY'. Must be 'page_index' or 'direct_version'"
    exit 1
fi

echo "=== RocksDB Benchmark Configuration ==="
echo "Database path: $DB_PATH"
echo "Storage strategy: $STRATEGY"
echo "Clean existing data: $CLEAN_DATA"
if [[ -n "$INITIAL_RECORDS" ]]; then
    echo "Initial records: ${INITIAL_RECORDS#* }"
fi
if [[ -n "$HOTSPOT_UPDATES" ]]; then
    echo "Hotspot updates: ${HOTSPOT_UPDATES#* }"
fi
echo "======================================="

# Check if executable exists
if [ ! -f "./build/src/rocksdb_bench_app" ]; then
    echo "Error: Executable not found. Please run ./scripts/build.sh first."
    exit 1
fi

# Build command arguments
CMD_ARGS="--db_path $DB_PATH --strategy $STRATEGY"

if [[ "$CLEAN_DATA" == true ]]; then
    CMD_ARGS="$CMD_ARGS --clean_data"
fi

if [[ -n "$INITIAL_RECORDS" ]]; then
    CMD_ARGS="$CMD_ARGS $INITIAL_RECORDS"
fi

if [[ -n "$HOTSPOT_UPDATES" ]]; then
    CMD_ARGS="$CMD_ARGS $HOTSPOT_UPDATES"
fi

echo "Starting benchmark..."
./build/src/rocksdb_bench_app $CMD_ARGS