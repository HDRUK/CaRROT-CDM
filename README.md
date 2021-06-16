# CO-CONNECT Tools

Welcome to our repo for `python` tools used by/with the CO-CONNECT project

### Table of Contents
1. [Installing](#installing)
1. [ETL Quick Start](#quick)
1. [CLI](#cli)



## Installing

This package runs with python versions `>=3.6`, the easiest way to install is via pip:
```
$ pip install co-connect-tools
```
To ensure the right version of python (python3) is used to install from pip you can do:
```
$ pip3 install co-connect-tools
```
Or to be even safer:
```
$ python3 -m pip install co-connect-tools
```

### From Source

To install from the source code, you can do:
```
$ git clone https://github.com/CO-CONNECT/co-connect-tools.git
$ cd co-connect-tools
$ pip install -e .
```

### Installing via yum
Package dependencies are located in `requirements.txt`, which need to be installed if you are building from source without pip:
```bash
$ cat requirements.txt 
numpy
pandas
coloredlogs
Jinja2
graphviz
click
```

## ETL Quick Start <a name="quick"></a>

The primary purpose of this package is running ETL of given a dataset and a set of transform rules encoded within a `json` file. The simplest way to run the ETLTool, designed to handle the output `json` of the CO-CONNECT Mapping-Pipeline web-tool, is to use the script `process_rules.py`

### Setup 

To run this example, obtain the location of the coconnect data folder, and set this as an environment variable for ease.
```
export COCONNECT_DATA_FOLDER=$(coconnect info data_folder)
```

### Execute

The example dataset and associated mapping rules can be run with the simple script `etlcdm.py`:
```bash
etlcdm.py -i $COCONNECT_DATA_FOLDER/test/inputs/*.csv --rules $COCONNECT_DATA_FOLDER/test/rules/rules_14June2021.json -o test/
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

