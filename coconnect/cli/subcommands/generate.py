import os
import click
import coconnect
import pandas as pd
    
@click.group(help='Commands to generate helpful files.')
def generate():
    pass

@click.command(help="generate a python configuration for the given table")
@click.argument("table")
@click.argument("version")
def cdm(table,version):
    data_dir = os.path.dirname(coconnect.__file__)
    data_dir = f'{data_dir}/data/'

    version = 'v'+version.replace(".","_")
    
    #load the details of this cdm objects from the data files taken from OHDSI GitHub
    # - set the table (e.g. person, condition_occurrence,...)  as the index
    #   so that all values associated with the object (name) can be retrieved
    # - then set the field (e.g. person_id, birth_datetime,,) to help with future lookups
    # - just keep information on if the field is required (Yes/No) and what the datatype is (INTEGER,..)
    cdm = pd.read_csv(f'{data_dir}/cdm/OMOP_CDM_{version}.csv',encoding="ISO-8859-1")\
                 .set_index('table')\
                 .loc[table].set_index('field')[['required', 'type']]

    for index,row in cdm.iterrows():
        required = row['required'] == "Yes"
        dtype = row['type']
        string = f'self.{index} = DataType(dtype="{dtype}", required={required})'
        print (string)
    
generate.add_command(cdm,"cdm")
