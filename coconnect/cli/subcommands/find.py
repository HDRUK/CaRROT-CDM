import os
import inspect
import click
import json
import glob
import coconnect
import coconnect.tools as tools
    
@click.group()
def find():
    pass

@click.command(help="Lookup some omop rules from a concept ID")
@click.argument("concept_id")
def rules(concept_id):
    from coconnect.tools.omop_db_inspect import OMOPDetails
    from dotenv import load_dotenv
    load_dotenv()
    tool = OMOPDetails()
    print (tool.get_rules(int(concept_id)))
    
find.add_command(rules,"rules")
