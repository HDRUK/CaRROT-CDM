# Helper Scripts

This folder contains scripts showing examples of how you can implement your own script to use the python CommonDataModel to build a model and execute a transformation of a dataset into the OHDSI CDM.

## etlcdm.py

The help message for this script displays:
```
usage: etlcdm.py [-h] --rules RULES --out-dir OUT_DIR --inputs INPUTS [INPUTS ...] [-nc NUMBER_OF_ROWS_PER_CHUNK]
                 [-np NUMBER_OF_ROWS_TO_PROCESS] [--use-profiler]

ETL-CDM: transform a dataset into a CommonDataModel.

optional arguments:
  -h, --help            show this help message and exit
  --rules RULES         input .json file
  --out-dir OUT_DIR, -o OUT_DIR
                        name of the output folder
  --inputs INPUTS [INPUTS ...], -i INPUTS [INPUTS ...]
                        input csv files
  -nc NUMBER_OF_ROWS_PER_CHUNK, --number-of-rows-per-chunk NUMBER_OF_ROWS_PER_CHUNK
                        choose to chunk running the data into nrows
  -np NUMBER_OF_ROWS_TO_PROCESS, --number-of-rows-to-process NUMBER_OF_ROWS_TO_PROCESS
                        the total number of rows to process
  --use-profiler        turn on saving statistics for profiling CPU and memory usage
```

For example, to execute this script run:
```bash
etlcdm.py -i <input file 1> <input file 2> .... <input file N>  --rules <json rules>  -o <location of output folder>
```