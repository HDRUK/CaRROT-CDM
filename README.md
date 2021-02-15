# ETLTool

```
usage: etl2cdm.py [-h] --inputs INPUTS [INPUTS ...] [--output-folder OUTPUT_FOLDER] --term-mapping
                  TERM_MAPPING --structural-mapping STRUCTURAL_MAPPING [-v]

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
  -v, --verbose         set debugging level

```


## Running locally

To run this tool locally, you'll need the following packages installed:
```
numpy
pandas
coloredlogs
```
Which can be installed via `pip install -r requirements.txt`

Then you can try out the sample input data:

```bash
python etl2cdm.py -v \
       --inputs sample_input_data/patients_sample.csv\
       --structural-mapping sample_input_data/rules1.csv\
       --term-mapping sample_input_data/rules2.csv 
```
Which by default will create files in a folder called `data/`

## Running with Docker

First you need to build the docker image
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

