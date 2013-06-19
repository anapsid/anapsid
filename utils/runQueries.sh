#!/bin/bash

if [ "$#" -lt 5 ]; then
    echo "Usage: $0 [query_folder] [endpoints_description] [heuristic] [result_file] [errors_files]"
    exit 1
fi

for query in `ls $1/*`; do
    (timeout -s 12 300 run_anapsid -e $2 -q $query -p b -s False -b 16384 -o False -d $3 -a True) 2>> $5 >> $4;
done;
