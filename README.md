# CO-CONNECT Tools

Welcome to our repo for `python` tools used by/with the CO-CONNECT project

### Table of Contents
1. [Installing](#installing)
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
$ coconnect map show example/sample_config/panther_structural_mapping.json 
{
      "person": [
            {
                  "day_of_birth": {
                        "source_table": "tracker.csv",
                        "source_field": "dob",
                        "term_mapping": null
                  },
		  ...
```

To display this `json` as a `dag` you can run:
```
$ coconnect map show example/sample_config/panther_structural_mapping.json 
```

The next step is to create a `.py` configuration file from this input `json`. The tool automatically registers these files, to see registred files, you can run:
```
$ coconnect map list
{}
```
Showing that no files have been created and registered.

To create your first configuration file, run `make` specifying the name of the output file/class:
```
$ coconnect map make --name Panther  example/sample_config/panther_structural_mapping.json 
Making new file /Users/calummacdonald/Usher/CO-CONNECT/Software/co-connect-tools/coconnect/cdm/classes/Panther.py

Looking at the file that has been created, you can see the `.py` configuration file has been made:
```python
from coconnect.cdm import define_person, define_condition_occurrence, load_csv
from coconnect.cdm import CommonDataModel
import json

class Panther(CommonDataModel):

    
    @define_person
    def person_0(self):
        self.day_of_birth = self.inputs["tracker.csv"]["dob"]
        self.gender_concept_id = self.inputs["demographic.csv"]["gender"]
        self.gender_source_concept_id = self.inputs["demographic.csv"]["gender"]
        self.gender_source_value = self.inputs["demographic.csv"]["gender"]
	...
```

Now the `list` command shows that the file has been registered with the tool:
```
$ coconnect map list
{
      "Panther": {
            "module": "coconnect.cdm.classes.Panther",
            "path": "/Users/calummacdonald/Usher/CO-CONNECT/Software/co-connect-tools/coconnect/cdm/classes/Panther.py",
            "last-modified": "2021-03-12 10:34:43"
      }
}
```

Now we're able to run:
```
$coconnect map run --name Panther example/sample_input_data/*.csv
...
2021-03-12 10:37:30 - Panther - INFO - saving person to output_data//person.csv
2021-03-12 10:37:30 - Panther - INFO -            gender_concept_id  year_of_birth  ...  race_source_concept_id  ethnicity_source_value
person_id                                    ...                                                
1                       8532           1962  ...                 4196428                        
2                       8532           1992  ...                 4196428                        
```

Outputs are saved in the folder `output_data`


______

## **DECREPIT** ETLTool

A tool for converting a ETL to a CDM, as used by the UK [CO-CONNECT](https://co-connect.ac.uk) project.
```
usage: etl2cdm [-h] --inputs INPUTS [INPUTS ...] [--output-folder OUTPUT_FOLDER] [--term-mapping TERM_MAPPING]
               --structural-mapping STRUCTURAL_MAPPING [--chunk-size CHUNK_SIZE] [-v]

Tool for mapping datasets

optional arguments:
  -h, --help            show this help message and exit
  --inputs INPUTS [INPUTS ...], -i INPUTS [INPUTS ...]
                        input .csv files for the original data to be mapped to the CDM
  --output-folder OUTPUT_FOLDER, -o OUTPUT_FOLDER
                        location of where to store the data
  --term-mapping TERM_MAPPING, -tm TERM_MAPPING
                        file that will handle the term mapping
  --structural-mapping STRUCTURAL_MAPPING, -sm STRUCTURAL_MAPPING
                        file that will handle the structural mapping
  --chunk-size CHUNK_SIZE
                        define how to "chunk" the dataframes, this specifies how many rows in the csv files to read
                        in at a time
  -v, --verbose         set debugging level
```


### Installing

To install the tools, you can use `pip`
```
pip install co-connect-tools
```

Otherwise, to install locally, checkout the package, navigate to the directory containing the source code and run
```
pip install -e .
```


### Running etl2cdm locally

To run this tool contained within the co-connect-tools package

```bash
etl2cdm -v \
       --inputs sample_input_data/patients_sample.csv\
       --structural-mapping sample_input_data/rules1.csv\
       --term-mapping sample_input_data/rules2.csv 
```
Which by default will create files in a folder called `data/`

### Running with Docker

First you need to build the docker image from this source

```bash
docker build . -t etltool:latest
```

Running
```bash
docker run -it -v `pwd`:/data/ etltool:latest\
            --inputs /data/sample_input_data/patients_sample.csv\
	    --structural-mapping /data/sample_input_data/rules1.csv\
	    --term-mapping /data/sample_input_data/rules2.csv\
	    --output-folder /data/output_data/
```

* `-v <folder>:/data/`: will mount any local folder containing the data
   * the container will then be able to see local files under `/data/`, and therefore you should specify additional commands to point to that root directory instead.

