# CO-CONNECT Tools

Welcome to our repo for `python` tools used by/with the CO-CONNECT project

### Table of Contents
1. [Installing](#installing)
1. [Quick Start](#quick)
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
Package dependancies are located in `requirements.txt`, which need to be installed if you are building from source without pip:
```bash
$ cat requirements.txt 
numpy
pandas
coloredlogs
Jinja2
graphviz
click
```


## Quick Start <a name="quick"></a>

Example:
```
coconnect map run --name Lion --rules example/sample_config/lion_structural_mapping.json  example/sample_input_data/*.csv
```

To run in one command, supply the name of the dataset (e.g. Panther), the rules `json` file that has been obtained from mapping-pipeline and then all the input files to run on.
```bash
coconnect map run --name <NAME> --rules <RULES.json> <INPUTFILE 1> <INPUTFILE 2> ....
```


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

The next step is to create a `.py` configuration file from this input `json`. The tool automatically registers these files, to see registred files, you can run:
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

