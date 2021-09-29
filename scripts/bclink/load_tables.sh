#!/bin/bash

data_file=$1
tablename=$2
#jobname=$3

dataset_tool --load --table=${tablename} --user=data \
    --data_file=${data_file} --support  --bcqueue \
#    --bcqueue-res-path=/data/var/lib/bcos/download/data/logs/mytest/ bclink
#    --bcqueue-res-path=./logs/${tablename} bclink


#do something with the masked_id file
#head ${output_folder}/masked_person_ids.csv

