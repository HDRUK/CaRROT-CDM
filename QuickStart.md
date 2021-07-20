
## Transforming data to CDM

### 1. Install the co-connect-tools python package

This package runs with `python` versions `>=3.6`, the easiest way to install is via pip:
```
$ python3 -m pip install co-connect-tools
```

!!! warning
    The tool is stable on (latest) Unix releases such as MacOS, Ubuntu, Centos. If you are using Windows, you may encounter problems.


If you are struggling to install from `pip` due to permissions, you can install as a user via:
```
$  python3 -m pip install co-connect-tools --user
```
Or, you can download the source code from [https://github.com/CO-CONNECT/co-connect-tools/tags](https://github.com/CO-CONNECT/co-connect-tools/tags), unpack and install as a local package:
```
$ cd < downloaded source code folder >
$ python3 -m pip install -e . 
```

!!! note
    If you have trouble with `pip` hanging on installing dependencies, try to install also using the argument `--no-cache-dir`. Also make sure that you have updated `pip` via `pip3 install --upgrade pip`.


### 2. Checking the package


To verify the package is installed you can test the following information commands:
```
$ coconnect info version
<tool version in format X.Y.Z>

$ coconnect info install_folder
<path to install folder>

```

### 3. Gather inputs

To run the transformation to CDM you will need:
1. Input Data
2. `json` file containing the so-called mapping rules

#### File checks

Input data is expected in `csv` format.

It is possible to do a quick check to display the first 10 rows of an input `csv`.
Run:
```
$ coconnect display dataframe --head 10 data/file_0.csv
<displays dataframe>
```


With your `json` file for the rules, you can quickly check the tool is able to read and display them via:
```
$ coconnect display json rules.json
```

### 4. Run the tool
```
$ coconnect map run --name <Name> --rules <.json file for rules> <csv files>
```
E.g.:
```
$ coconnect map run --name TestData --rules rules.json data/*.csv
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

## Common Problems

The following documents common error messages and FAQs.

### Why do I get a `KeyError` message about my input data?

You may see a look up error like this, when running the tool:
```
    self.observation_concept_id.series = self.inputs["FILE0.csv"]["column_name_0"]
KeyError: 'FILE0.csv'
```

This tells you that when running the tool you have not supplied the input file `FILE0.csv`.

It is crucial that the names of the files encocded in the `json` file match those that are supplied to the tool, otherwise the tool cannot run.

A common problem is the capitalisation of the file names, or the file names in the `json` file missing a `.csv` extension.

In other words, whatever file names are used in the `json` file under the section `"source_table":` must be supplied as inputs.



