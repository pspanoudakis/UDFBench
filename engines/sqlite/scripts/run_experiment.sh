#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -lt 9 ]; then
    echo "Usage: $0 <database_file> <threads> <cache> <disk> <collectl> <datasetpath> <externalpath> <pythonexec> <query_file1> [<query_file2> ...]"
    exit 1
fi

DATABASE="$1"
shift


if [ ! -d "$SQLITERESULTSPATH" ]; then
    mkdir -p "$SQLITERESULTSPATH"
    mkdir -p "$SQLITERESULTSPATH/experiments"
    mkdir -p "$SQLITERESULTSPATH/collectl"
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


SQLITEDBPATH=$SQLITEDBSSDPATH

if [ $DISK = "ssd" ]; then
    true
elif [ $DISK = "hdd" ]; then
    SQLITEDBPATH=$SQLITEDBHDDPATH
elif [ $DISK = "mem" ]; then
    SQLITEDBPATH=$SQLITEDBMEMPATH
fi

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
    sudo swapoff -a
    sudo swapon -a

    repeats=1
  fi
  for ((i = 1; i <= $repeats; i++)); do
    query_number="${query_file##*/q}"
    query_number="${query_number%.sql}"
    filename="$DATABASE"-"$query_number"-t"$NTHREADS"-"$CACHE"-"$DISK"
    rm_output=$(rm -r "$SQLITERESULTSPATH/collectl/$filename"* 2>&1)

    if [ $COLLECTL = "true" ]; then 
        collectl -f "$SQLITERESULTSPATH/collectl/$filename" -scdnm -F0 -i.1 &
        COLLECTL_PID=$!
    fi
    { "$PYTHONEXEC" "$SQLITEPATH"  "$SQLITEDBPATH/$DATAB".db  "$query_file" "$SQLITEUDFS/scalar" "$SQLITEUDFS/aggregate"; } &> "$SQLITERESULTSPATH/experiments/$filename".txt
    if  [ $COLLECTL = "true" ]; then 
        kill $COLLECTL_PID
    fi

       
done
    if  [ $COLLECTL = "true" ]; then 
        collectl -p $SQLITERESULTSPATH/collectl/$filename*.gz -scdnm -P  > $SQLITERESULTSPATH/collectl/$filename.csv
    fi
    if [[ $query_number == '20' ]] && (( repeats % 2 != 0 )); then

        "$PYTHONEXEC" "$SQLITEPATH"  "$SQLITEDBPATH/$DATAB".db  "$query_file" "$SQLITEUDFS/scalar" "$SQLITEUDFS/aggregate" &>/dev/null

    fi
   
done
