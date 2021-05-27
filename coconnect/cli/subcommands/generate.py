import os
import click
import coconnect


    
@click.group()
def generate():
    pass

@click.command(help="generate a python configuration for the given table")
@click.argument("table")
def cdm(table):
    data_dir = os.path.dirname(coconnect.__file__)
    data_dir = f'{data_dir}/data/'
    
    #load the details of this cdm objects from the data files taken from OHDSI GitHub
    # - set the table (e.g. person, condition_occurrence,...)  as the index
    #   so that all values associated with the object (name) can be retrieved
    # - then set the field (e.g. person_id, birth_datetime,,) to help with future lookups
    # - just keep information on if the field is required (Yes/No) and what the datatype is (INTEGER,..)
    cdm = pd.read_csv(f'{data_dir}/cdm/OMOP_CDM_{_version}.csv',encoding="ISO-8859-1")\
                 .set_index('table')\
                 .loc[table].set_index('field')[['required', 'type']]

    print (cdm)
    
map.add_command(cdm,"cdm")
