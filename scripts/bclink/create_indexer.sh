#!/bin/bash

tables=$@

for table in $tables
do

    query="SELECT count(*) FROM ${table}"
    count=$(bc_sqlselect --user=bclink bclink --query="${query}" | tail -n +2)

    if [[ $count -eq 0 ]]; then
	continue
    fi

    query="SELECT column_name \
FROM INFORMATION_SCHEMA. COLUMNS \
WHERE table_name = '"$table"' LIMIT 1 "
    pk=$(bc_sqlselect --user=bclink bclink --query="${query}" | tail -n +2 )

    query="SELECT ${pk} from ${table} ORDER BY -${pk} LIMIT 1; "
    last_index=$(bc_sqlselect --user=bclink bclink --query="${query}" | tail -n +2)
    let "last_index+=1" 
    echo ${table%%$instance},$last_index 
done
