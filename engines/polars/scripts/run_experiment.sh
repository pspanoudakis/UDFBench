#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -lt 9 ]; then
    echo "Usage: $0 <database_file> <threads> <cache> <disk> <collectl> <datasetpath> <externalpath> <pythonexec> <query_file1> [<query_file2> ...]"
    exit 1
fi

DATABASE="$1"
shift


if [ ! -d "$POLARSRESULTSPATH" ]; then
    mkdir -p "$POLARSRESULTSPATH"
    mkdir -p "$POLARSRESULTSPATH/experiments"
    mkdir -p "$POLARSRESULTSPATH/collectl"
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
            ;;
    esac

NTHREADS="$1"

shift
CACHE="$1"
shift
DISK="$1"
shift

COLLECTL="$1"
shift

DATASETSPATH="$1"
shift

EXTERNALPATH="$1"
shift

PYTHONEXEC="$1"
shift


arr=("$@")

# Iterate over each element in the array
for query_file in "${arr[@]}"; do

    repeats=1
    if [ $CACHE = "hot" ]; then
        repeats=2
    else

        sudo sync; sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'
        sudo sync; sudo sh -c 'echo 2 > /proc/sys/vm/drop_caches'
        sudo sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        echo 3 | sudo  tee /proc/sys/vm/drop_caches
        sudo rm -rf "$POLARSQUERIES/__pycache__"
        sudo rm -rf "$POLARSSCRIPTS/__pycache__"
        sudo rm -rf "$POLARSSCALAR/__pycache__"
        sudo rm -rf "$POLARSAGGR/__pycache__"
        sudo rm -rf "$POLARSTABLE/__pycache__"
        sudo swapoff -a
        sudo swapon -a

        repeats=1
    fi
    for ((i = 1; i <= $repeats; i++)); do
        query_number="${query_file##*/q}"
        query_number="${query_number%.py}"
        filename="$DATABASE"-"$query_number"-t"$NTHREADS"-"$CACHE"-"$DISK"
        rm_output=$(rm -r "$POLARSRESULTSPATH/collectl/$filename"* 2>&1)

        if [ $COLLECTL = "true" ]; then 
            collectl -f "$POLARSRESULTSPATH/collectl/$filename" -scdnm -F0 -i.1 &
            COLLECTL_PID=$!
        fi

        if [ $POLARSDATAFORMAT = "csv" ]; then 
            POLARSDATAPATH="$DATASETSPATH/csvs/$DATAB"
        elif [ $POLARSDATAFORMAT = "parquet" ]; then 
            POLARSDATAPATH="$DATASETSPATH/parquet/$DATAB"
        else
            echo "Unsupported Polars data format: $POLARSDATAFORMAT"
            exit 1
        fi
        EXTDATAPATH="$EXTERNALPATH/$DATAB"

        { "$PYTHONEXEC" "$POLARSPATH" "$POLARSSCHEMA" "$POLARSDATAPATH" "$POLARSDATAFORMAT" "$EXTDATAPATH" "$query_file" "$POLARSSCALAR" "$POLARSAGGR" "$POLARSTABLE"; } &> "$POLARSRESULTSPATH/experiments/$filename".txt

        if  [ $COLLECTL = "true" ]; then 
            kill $COLLECTL_PID
        fi
        
    done
done