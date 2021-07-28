## Command Line Interface <a name="cli"></a>

To list the available commands available with the `cli`, you can simply use `--help`:
```
$ coconnect --help
Usage: coconnect [OPTIONS] COMMAND [ARGS]...

Options:
  -l, --loglevel TEXT
  --help               Show this message and exit.

Commands:
  display   Commands for displaying various types of data and files.
  generate  Commands to generate helpful files.
  info      Commands to find information about the package.
  map       Commands for mapping data to the OMOP CommonDataModel (CDM)
```

### Info
Info commands show a few useful pieces of information to check the tool is installed and working correctly.

```
Commands:
  data_folder     Get the data folder location
  install_folder  Get the root folder location of coconnect tools
  version         Get the installed version of the package'
```

### Generate

```
$ coconnect generate --help
Usage: coconnect generate [OPTIONS] COMMAND [ARGS]...

  Commands to generate helpful files.

Options:
  --help  Show this message and exit.

Commands:
  cdm        generate a python configuration for the given table
  synthetic  generate synthetic data from a ScanReport
```

These two commands are useful for development of the tool and testing the tool on fake (synthetic) data


### Display

Various helper functions for displaying data are found with the `display` command:
```
$ coconnect display --help
Usage: coconnect display [OPTIONS] COMMAND [ARGS]...

  Commands for displaying various types of data and files.

Options:
  --help  Show this message and exit.

Commands:
  dag        Display the OMOP mapping json as a DAG
  dataframe  Display a dataframe
  diff       Detect differences in either inputs or output csv files
  flatten    flattern a rules json file
  json       Show the OMOP mapping json
  plot       plot from a csv file
```

#### Example
This quick example shows how you can run OMOP mapping based on a sample json file

Firstly run `show` to display the input `json` structure
```
$ coconnect display json example/sample_config/lion_structural_mapping.json
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

To display this `json` as a `dag` (directed acyclic graph) you can run:
```
$ coconnect display dag example/sample_config/lion_structural_mapping.json 
```

### Map

Commands used for mapping datasets with OMOP are found with the command `map`
```
$ coconnect map --help
Usage: coconnect map [OPTIONS] COMMAND [ARGS]...

  Commands for mapping data to the OMOP CommonDataModel (CDM).

Options:
  --help  Show this message and exit.

Commands:
  py   Commands for using python configurations to run the ETL...
  run  Perform OMOP Mapping given an json file
```  

Using `py` command inserts an intermediate step in which the `json` file can be converted into a `py` file which defines the CDM tables. This allows a user to have more control over the structural mapping.

To create a `.py` configuration file from this input `json`. The tool automatically registers these files, to see registered files, you can run:
```
$ coconnect map py list
{}
```
Showing that no files have been created and registered.

To create your first configuration file, run `make` specifying the name of the output file/class:
```
$ coconnect map py make --name Lion  example/sample_config/lion_structural_mapping.json --register
```
Using the argument `--register` will register the configuration file via a symbolic link.

If you don't want to register the configuration with the tool via the symbolic link (maybe you don't have the sudo permissions to do this, if you did not install the tool yourself), you can instead export an environment variable to your working directory:
```
export COCONNECT_CONFIG_FOLDER=`pwd`
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

Now we're able to run:
```
$ coconnect map py run --pyconf Lion.py example/sample_input_data/*.csv
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
