#!/bin/bash

tables=$@

for table in $tables;
do
   datasettool2 delete-all-rows $table --database=bclink
done
