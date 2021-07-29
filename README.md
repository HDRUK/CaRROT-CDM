# CO-CONNECT Tools

Welcome to our repo for `python` tools used by/with the CO-CONNECT project, primarily for performing ETL on health datasets, converting them to the OHDSI CommonDataModel.

CO-CONNECT-Tools contains a pythonic version of the OHDSI CDM (default version 5.3.1). CLI tools can be used to define and process a CDM model given input data and given a `json` rules file.

### Table of Contents
1. [Installing](#installing)
1. [ETL-CDM Quick Start](#quick)


## Installing

!!! caution
    This tool is only stable and can run with python versions `>=3.6` on the latest Unix distributions (macOS, Ubuntu, Centos7) and on Windows. 

To install the package, the easiest way is to do this is via pip:
```
$ python -m pip install co-connect-tools
```

To ensure the right version of python (python3) is used to install from pip you can also do:
```
$ python3 -m pip install co-connect-tools
```
Or
```
$ pip3 install co-connect-tools
```

If you are struggling to install from `pip` due to lack of root permissions, or you are not using a virtual python environment (e.g. conda), you can install as a user with the command:
```
$  python3 -m pip install co-connect-tools --user
```
This will install the package into a local user folder.

Alterative you can [download the source code](https://github.com/CO-CONNECT/co-connect-tools/tags), unpack and install as a local package:
```
$ cd < downloaded source code folder >
$ python3 -m pip install -e . 
```

!!! tip
    If you have trouble with `pip` hanging on installing dependencies, try to install using the argument `--no-cache-dir`. Also make sure that you have updated `pip` via `pip3 install --upgrade pip`.


### Manual Install 
If you are on a system and need to install manual without pip (e.g. yum), you can install from source but will also need to install the additional dependencies that are located in `requirements.txt`:

```bash
$ cat requirements.txt 
numpy
pandas
coloredlogs
Jinja2
graphviz
click
sqlalchemy
tabulate
psutil
```

## ETL-CDM Quick Start <a name="quick"></a>

The primary purpose of this package is running ETL of given a dataset and a set of transform rules encoded within a `json` file. The simplest way to run the ETLTool, designed to handle the output `json` of the CO-CONNECT Mapping-Pipeline web-tool.


### 1. Checking The Package

To verify the package is installed you can test the following information commands:
```
$ coconnect info version
<tool version in format X.Y.Z>

$ coconnect info install_folder
<path to install folder>

```

### 2. Gather Inputs

To run the transformation to CDM you will need:   

1. Input Data  
1. `json` file containing the so-called mapping rules


### 3. Check Inputs

Input data is expected in `csv` format.

It is possible to do a quick check to display the first 10 rows of an input `csv`.
Run:
```
$ coconnect display dataframe --head 10 <input data csv file>
```

With your `json` file for the rules, you can quickly check the tool is able to read and display them via:
```
$ coconnect display json rules.json
```

### 4. Run The Tool

The synthax for running the tool can be seen from using `--help`:
```
$ coconnect map run --help
Usage: coconnect map run [OPTIONS] [INPUTS]...

  Perform OMOP Mapping given an json file

Options:
  --rules TEXT                    input json file containing all the mapping
                                  rules to be applied  [required]

  --type [csv]                    specify the type of inputs, the default is
                                  .csv inputs

  --use-profiler                  turn on saving statistics for profiling CPU
                                  and memory usage

  --output-folder TEXT            define the output folder where to dump csv
                                  files to

  -nc, --number-of-rows-per-chunk INTEGER
                                  choose to chunk running the data into nrows
  -np, --number-of-rows-to-process INTEGER
                                  the total number of rows to process
  --help                          Show this message and exit.
```

The tool ==requires== you to pass a `.json` file for the rules, as well as space separated list of `.csv` files 

```
$ coconnect map run --rules <.json file for rules> <csv file 1> <csv file 2> <csv file 3> ...
```


!!! example

    === "Unix Users"
    
        For macOS/Ubuntu/Centos etc. users, you can easily run from the CLI with a wildcard. Assuming your input data is located in the folder `data/` you can run:


        ``` bash
    	$ coconnect map run --rules rules.json data/*.csv
        ```

    	The tool has the capability to also run on a folder containing the `.csv` files. The tool will look in the folder for `.csv` files and load them:

        ``` bash
    	$ coconnect map run --rules rules.json data/
        ```


    === "Windows Users"
    
        For Windows users, you can run by passing the full-path to the folder containing your `.csv` input files.


        ``` 
    	> coconnect map run --rules rules.json D:\Foo\Bar\data
        ```

        Or by manually passing the individual input csv files:
        
        ``` 
        > coconnect map run --rules rules.json D:\Foo\Bar\data\file_1.csv D:\Foo\Bar\data\file_2.csv
        ```

        Wildcards for inputs ....
        {== THIS NEEDS INSTRUCTIONS FOR WINDOWS USERS ==}
	
### 5. Check The Output

By default, mapped `csv` files are created in the folder `output_data` within your current working directory.
!!! tip
    To specify a different output folder, use the command line argument `--output-folder` when running `coconnect map run`

Log files are also created in a subdirectory of the output folder, for example:
```
output_data/
├── condition_occurrence.csv
├── logs
│   └── 2021-07-19T100054.json
└── observation.csv
```

Other than opening up the output csv in your favourite viewer, you can also use the command line tools to display a simple dataframe
```
$ coconnect display dataframe --drop-na output_data/condition_occurrence.csv 
       condition_occurrence_id  person_id  condition_concept_id  ... condition_end_datetime condition_source_value  condition_source_concept_id
0                            1          9                312437  ...    2020-04-10 00:00:00                      1                       312437
1                            2         18                312437  ...    2020-04-11 00:00:00                      1                       312437
2                            3         28                312437  ...    2020-04-10 00:00:00                      1                       312437
3                            4         38                312437  ...    2020-04-10 00:00:00                      1                       312437
4                            5         44                312437  ...    2020-04-10 00:00:00                      1                       312437
```


Markdown format can be outputed for convenience too:
```
$ coconnect display dataframe --markdown --drop-na test/person.csv
```

|    |   person_id |   gender_concept_id | birth_datetime      | gender_source_value   |   gender_source_concept_id |
|---:|------------:|--------------------:|:--------------------|:----------------------|---------------------------:|
|  0 |         101 |                8507 | 1951-12-25 00:00:00 | M                     |                       8507 |
|  1 |         102 |                8507 | 1981-11-19 00:00:00 | M                     |                       8507 |
|  2 |         103 |                8532 | 1997-05-11 00:00:00 | F                     |                       8532 |
|  3 |         104 |                8532 | 1975-06-07 00:00:00 | F                     |                       8532 |
|  4 |         105 |                8532 | 1976-04-23 00:00:00 | F                     |                       8532 |
|  5 |         106 |                8507 | 1966-09-29 00:00:00 | M                     |                       8507 |
|  6 |         107 |                8532 | 1956-11-12 00:00:00 | F                     |                       8532 |
|  7 |         108 |                8507 | 1985-03-01 00:00:00 | M                     |                       8507 |
|  8 |         109 |                8532 | 1950-10-31 00:00:00 | F                     |                       8532 |
|  9 |         110 |                8532 | 1993-09-07 00:00:00 | F                     |                       8532 |


### 6. Additional Options

!!! note
    For large datasets, it's advised that you do not try to run all the data at the same time


To process a dataset in chunks, you can use the following flag:

```
  -nc, --number-of-rows-per-chunk INTEGER
                                  choose to chunk running the data into nrows
```

For a dataset of order millions of records, it is advised that you run with `-nc 100000` which will process the data in 100k row chunks. 



For testing or debugging purposes, especially if you are transforming a large dataset, you can use the option `-np` to only run on a certain number of initial rows of the input `csv` files. 
```
  -np, --number-of-rows-to-process INTEGER
                                  the total number of rows to process
```



If you wish to profile the CPU/memory usage as a function of time, you can run with the following flag:
```
  --use-profiler                  turn on saving statistics for profiling CPU
                                  and memory usage
```

Which will additionally save and output a time series from the start of executing to the end of executing the ETL.
```
2021-07-27 10:31:48 - Profiler - INFO -      time[s]  memory[GB]  cpu[%]
0   0.000384    0.056290   0.000
1   0.104976    0.057865  24.650
2   0.205735    0.058556  23.325
3   0.308194    0.060932  24.625
4   0.415116    0.061394  24.650
...
2021-07-27 10:31:48 - Profiler - INFO - finished profiling
2021-07-27 10:31:48 - CommonDataModel - INFO - Writen the memory/cpu statistics to /Users/calummacdonald/Usher/CO-CONNECT/Software/co-connect-tools/alspac/output_data//logs//statistics_2021-07-27T093143.csv
```
