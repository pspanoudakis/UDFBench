#!/bin/bash


source $PWD'/automations/config_udfbench.sh'

# Check if the correct number of arguments is provided
if [ "$#" -lt 6 ]; then
    echo "Usage: $0 <system> <database_file> <threads> <cache> <disk> <collectl> <queryfile>"
    exit 1
fi


engines=( $(find "$PWD"/engines/ -mindepth 1 -maxdepth 1 -type d | xargs -n 1 basename) )


SYSTEM="$1"
if [[ ! " ${engines[@]} " =~ " $SYSTEM " ]]; then
    echo "Invalid system. Please check that your system is in the right path (engines/)."
    exit 1
fi
shift


DATABASE="$1"
shift


if [ ! -d "$RESULTSPATH" ]; then
    mkdir -p "$RESULTSPATH"
    mkdir -p "$RESULTSPATH/logs"
fi

DATAB="tiny"
    case $DATABASE in
        t)
            DATAB="tiny"
            ;;
        s)
            DATAB="small"
            ;;
        m)
            DATAB="medium"
            ;;
        l)
            DATAB="large"
            ;;
        *)
            echo "Invalid input"
            exit 1
            ;;
    esac

THREADS="$1"
NTHREADS=$(echo "$THREADS" | grep -oE '[0-9]+')
shift

CACHE="$1"
shift

DISK="$1"

DATASETSPATH=$DATASETSPATHSSD
EXTERNALPATH=$EXTERNALPATHSSD
if [ $DISK = "ssd" ]; then
    true
elif [ $DISK = "hdd" ]; then
    DATASETSPATH=$DATASETSPATHHDD
    EXTERNALPATH=$EXTERNALPATHHDD
elif [ $DISK = "mem" ]; then
    DATASETSPATH=$DATASETSPATHMEM
    EXTERNALPATH=$EXTERNALPATHMEM
fi

shift

COLLECTL="$1"
shift

TARGET_DIR="$PWD"/engines/"$SYSTEM"/queries


if [ ! -d "$TARGET_DIR" ]; then
    echo "Directory not found: $TARGET_DIR"
    exit 1
fi

query_files=()
if [[ "$SYSTEM" == "pandas" || "$SYSTEM" == "polars" ]]; then
    # for pandas-like systems, we expect .py query files
    query_extension="py"
else
    # we assume .sql query files for others
    query_extension="sql"
fi

if [ "$#" -gt 0 ]; then
    for arg in "$@"; do
        query_file="$PWD/engines/$SYSTEM/queries/q$arg.$query_extension"
        if [ -f "$query_file" ]; then
            query_files+=("$(realpath "$query_file")") 
        else
            echo "Warning: '$arg' is not a valid query number. Skipping."
        fi
    done
else
    if [ ! -d "$TARGET_DIR" ]; then
        echo "Directory not found: $TARGET_DIR"
        exit 1
    fi

    while IFS= read -r -d $'\0' file; do
        full_path=$(realpath "$file") 
        query_files+=("$full_path")  
    done < <(find "$TARGET_DIR" -maxdepth 1 -type f -name "q*.$query_extension" -print0)

fi

if [ ${#query_files[@]} -eq 0 ]; then
    echo "No .$query_extension query files found in the directory: $TARGET_DIR"
    exit 1
fi

source "$PWD"/engines/"$SYSTEM"/scripts/"$SYSTEM"_config.sh

"$PWD"/engines/"$SYSTEM"/scripts/run_experiment.sh "$DATABASE" "$NTHREADS" "$CACHE" "$DISK" "$COLLECTL" "$DATASETSPATH" "$EXTERNALPATH" "$PYTHONEXEC" "${query_files[@]}"
