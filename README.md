# CO-CONNECT Tools

Welcome to our repo for `python` tools used by/with the CO-CONNECT project, primarily for performing ETL on health datasets, converting them to the OHDSI CommonDataModel.

CO-CONNECT-Tools contains a pythonic version of the OHDSI CDM (default version 5.3.1). CLI tools can be used to define and process a CDM model given input data and given a `json` rules file.

### Table of Contents
1. [Installing](#installing)
1. [ETL-CDM Quick Start](#quick)
1. [CLI](#cli)


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


### 1. Checking the package

To verify the package is installed you can test the following information commands:
```
$ coconnect info version
<tool version in format X.Y.Z>

$ coconnect info install_folder
<path to install folder>

```


### 2. Gather inputs

To run the transformation to CDM you will need:   

1. Input Data  
1. `json` file containing the so-called mapping rules


### 3. Check inputs

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

### 4. Run the tool

Pass the tool the rules `.json` file 

```
$ coconnect map run --rules <.json file for rules> <csv file 1> <csv file 2> <csv file 3> ...
```
E.g.:
```
$ coconnect map run --rules rules.json data/*.csv
```

### 5. Check the output

By default, mapped `csv` files are created in the folder `output_data` within your current working directory.
!!! note
    To specify a different output folder, use the command line argument `--output-folder` when running `coconnect map run`

Additionally, log files are created in a subdirectory of the output folder, for example:
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


### Inspecting the Output

`.csv` files are created for each CDM table, e.g. `person.csv`. Additionally logs are created and stored in the sub-folder `logs/`.
```
$ tree test/
test/
├── condition_occurrence.csv
├── logs
│   └── 2021-06-16T100657.json
├── observation.csv
└── person.csv
```

A convenience command exists to be able to display the output dataframe to the command-line:
```
$ coconnect display dataframe test/person.csv 
   person_id  gender_concept_id  ...  ethnicity_source_value  ethnicity_source_concept_id
0        101               8507  ...                     NaN                          NaN
1        102               8507  ...                     NaN                          NaN
2        103               8532  ...                     NaN                          NaN
3        104               8532  ...                     NaN                          NaN
4        105               8532  ...                     NaN                          NaN
5        106               8507  ...                     NaN                          NaN
6        107               8532  ...                     NaN                          NaN
7        108               8507  ...                     NaN                          NaN
8        109               8532  ...                     NaN                          NaN
9        110               8532  ...                     NaN                          NaN

[10 rows x 18 columns]
```
This can also be used with the option `--drop-na` to just display those columns which have none-NaN values
```
$ coconnect display dataframe --drop-na test/person.csv 
   person_id  gender_concept_id       birth_datetime gender_source_value  gender_source_concept_id
0        101               8507  1951-12-25 00:00:00                   M                      8507
1        102               8507  1981-11-19 00:00:00                   M                      8507
2        103               8532  1997-05-11 00:00:00                   F                      8532
3        104               8532  1975-06-07 00:00:00                   F                      8532
4        105               8532  1976-04-23 00:00:00                   F                      8532
5        106               8507  1966-09-29 00:00:00                   M                      8507
6        107               8532  1956-11-12 00:00:00                   F                      8532
7        108               8507  1985-03-01 00:00:00                   M                      8507
8        109               8532  1950-10-31 00:00:00                   F                      8532
9        110               8532  1993-09-07 00:00:00                   F                      8532
```

Markdown format can be obtained for convenience:
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



## Command Line Interface <a name="cli"></a>

To list the available commands available with the `cli`, you can simply use `--help`:
```bash
$ coconnect --help
Usage: coconnect [OPTIONS] COMMAND [ARGS]...

Options:
  -l, --loglevel TEXT
  --help               Show this message and exit.

Commands:
  map
```

### Map

Commands used for mapping datasets with OMOP are found with the command `map`
```bash
$ coconnect map --help
Usage: coconnect map [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  display  Display the OMOP mapping json as a DAG
  list     List all the python classes there are available to run
  make     Generate a python class from the OMOP mapping json
  run      Perform OMOP Mapping
  show     Show the OMOP mapping json
```  

#### Example
This quick example shows how you can run OMOP mapping based on a sample json file

Firstly run `show` to display the input `json` structure
```
$ coconnect map show example/sample_config/lion_structural_mapping.json
...
      "cdm": {
            "person": [
                  {
                        "birth_datetime": {
                              "source_table": "demo.csv",
                              "source_field": "dob",
                              "term_mapping": null,
                              "operations": [
                                    "get_datetime"
                              ]
                        },
			...
```

To display this `json` as a `dag` you can run:
```
$ coconnect map display example/sample_config/lion_structural_mapping.json 
```

The next step is to create a `.py` configuration file from this input `json`. The tool automatically registers these files, to see registered files, you can run:
```
$ coconnect map list
{}
```
Showing that no files have been created and registered.

To create your first configuration file, run `make` specifying the name of the output file/class:
```
$ coconnect map make --name Lion  example/sample_config/lion_structural_mapping.json
Making new file <your install dir>/co-connect-tools/coconnect/cdm/classes/Lion.py
```

Looking at the file that has been created, you can see the `.py` configuration file has been made:


```python
from coconnect.cdm import define_person, define_condition_occurrence, define_visit_occurrence, define_measurement, define_observation
from coconnect.cdm import CommonDataModel
import json

class Lion(CommonDataModel):
    
...
    @define_person
    def person_0(self):
        """
        Create CDM object for person
        """
        self.birth_datetime = self.inputs["demo.csv"]["dob"]
        self.day_of_birth = self.inputs["demo.csv"]["dob"]
        self.gender_concept_id = self.inputs["demo.csv"]["gender"]
        self.gender_source_concept_id = self.inputs["demo.csv"]["gender"]
        self.gender_source_value = self.inputs["demo.csv"]["gender"]
        self.month_of_birth = self.inputs["demo.csv"]["dob"]
....

```

Now the `list` command shows that the file has been registered with the tool:
```
$ coconnect map list
{
      "Lion": {
            "module": "coconnect.cdm.classes.Lion",
            "path": "/Users/calummacdonald/Usher/CO-CONNECT/Software/co-connect-tools/coconnect/cdm/classes/Lion.py",
            "last-modified": "2021-04-16 10:26:33"
      }
}
```

Now we're able to run:
```
$ coconnect map run --name Lion example/sample_input_data/*.csv
...

2021-04-16 10:29:12 - Lion - INFO - finalised observation
2021-04-16 10:29:12 - Lion - INFO - saving person to output_data//person.csv
2021-04-16 10:29:12 - Lion - INFO -            gender_concept_id  year_of_birth  ...  race_source_concept_id  ethnicity_source_value
person_id                                    ...                                                
1                       8532           1962  ...                  123456                        
2                       8507           1972  ...                  123456                        
3                       8532           1979  ...                  123422                        
4                       8532           1991  ...                  123456                        

[4 rows x 12 columns]
...
```

Outputs are saved in the folder `output_data`


### All in One

In one command, all the above steps can be executed as such:

Example:
```
coconnect map run --name Lion --rules example/sample_config/lion_structural_mapping.json  example/sample_input_data/*.csv
```

To run in one command, supply the name of the dataset (e.g. Panther), the rules `json` file that has been obtained from mapping-pipeline and then all the input files to run on.
```bash
coconnect map run --name <NAME> --rules <RULES.json> <INPUTFILE 1> <INPUTFILE 2> ....
```

