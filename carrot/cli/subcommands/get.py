import os
import click
import json
import carrot
import pandas as pd
import numpy as np
import requests
from dotenv import dotenv_values
    
class MissingToken(Exception):
    pass


@click.group(help='Commands to get data from the CCOM api.')
@click.option("-t","--token",help="specify the carrot_token for accessing the CCOM website",type=str,default=None)
@click.option("-u","--url",help="url endpoint for the CCOM website to ping",
              type=str,
              default=None)
@click.pass_context
def get(ctx,token,url):
    config = dotenv_values(".env")
    if token:
        config['CCOM_TOKEN'] = token
    if url:
        config['CCOM_URL'] = url

    if 'CCOM_TOKEN' not in config:
        raise MissingToken("you must use the option --token or create a .env file containing CCOM_TOKEN to be able to use this functionality.")

    token = config['CCOM_TOKEN']
    config['headers'] = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        "Content-type": "application/json",
        "charset": "utf-8",
        "Authorization": f"Token {token}"
    }
    ctx.obj = config
    

@click.command(help="get information about a concept")
@click.argument("concept_id",required=True)
@click.pass_obj
def concept(config,concept_id):
    url = config['CCOM_URL']
    headers=config['headers'] 
    response = requests.get(
        f"{url}/api/omop/concepts/{concept_id}/",
        headers=config['headers']
    )
    res = response.json()
    click.echo(json.dumps(response.json(),indent=6))

@click.command(help="get all person_ids used")
@click.pass_obj
def person_ids(config):
    url = config['CCOM_URL']
    headers=config['headers']
    response = requests.get(
        f"{url}/api/scanreporttablesfilter/",
        headers=headers
    )
    person_ids = ','.join([str(x['person_id']) for x in response.json() if x['person_id']])
    response = requests.get(
        f"{url}/api/scanreportfieldsfilter/?id__in={person_ids}&fields=name",
        headers=headers
    )
    click.echo(list(set([x['name'].replace("\ufeff","") for x in response.json()])))
    
@click.command(help="get all date_events used")
@click.pass_obj
def date_events(config):
    url = config['CCOM_URL']
    headers=config['headers']
    response = requests.get(
        f"{url}/api/scanreporttablesfilter/",
        headers=headers
    )
    date_events = ','.join([str(x['date_event']) for x in response.json() if x['date_event']])
    response = requests.get(
        f"{url}/api/scanreportfieldsfilter/?id__in={date_events}&fields=name",
        headers=headers
    )
    click.echo(list(set([x['name'].replace("\ufeff","") for x in response.json()])))
        

@click.command(help="get a report of the concepts that have been mapped ")
@click.option("--flat",help="do one object for each concept (dont group)",is_flag=True)
@click.pass_obj
def concepts(config,flat):

    token = config['CCOM_TOKEN']
    url = config['CCOM_URL']
    headers = config['headers']
 
    response = requests.get(
        f"{url}/api/scanreports",
        headers=headers
    )
    scan_report_ids = [
        report['id']
        for report in response.json()
        if report['hidden'] == False
    ]

    all_rules = []
    for _id in scan_report_ids:
        response = requests.get(
            f"{url}/api/json/?id={_id}",
            headers=headers
        )
        try:
            all_rules.append(response.json()[0])
        except:
            pass

        
    
    inverted = {}
    _list = []
    for rules in all_rules:
    
        source_dataset = rules['metadata']['dataset']
        
        for cdm_table_name,table in rules['cdm'].items():
            for concept_name,concept_group in table.items():
                for cdm_field_name,field in concept_group.items():
                    if not 'term_mapping' in field:
                        continue

                    source_table = field['source_table']
                    source_field = field['source_field']
                    
                    term_mapping = field['term_mapping']
                    
                    if not isinstance(term_mapping,dict):
                        concept_ids = [(None,term_mapping)]
                    else:
                        concept_ids = list(term_mapping.items())

                                             
                    for source_value,concept_id in concept_ids:

                        if concept_id not in inverted:
                            inverted[concept_id] = {
                                'concept_name':concept_name.rsplit(' ',1)[0],
                                'domain':cdm_table_name,
                                'sources':[]
                            }


                        #if concept_id not in inverted:
                        #    inverted[concept_id] = []
                        #else:
                        #    print (concept_id)
                        #    print (inverted[concept_id])
                        #    exit(0)
                            
                        obj = {
                            'source_field':source_field,
                            'source_table':source_table,
                            'source_dataset':source_dataset
                        }
                        if source_value is not None:
                            obj['source_value'] = source_value

                        temp = {
                            'concept_id':concept_id,
                            'concept_name':concept_name,
                            'domain':cdm_table_name,
                            **obj
                        }
                        _list.append(temp)
                            
                        if obj not in inverted[concept_id]['sources']:
                            inverted[concept_id]['sources'].append(obj)

    inverted = [
        {'concept_id':_id,**obj}
        for _id,obj in inverted.items()
    ]

    if not flat:
        print (json.dumps(inverted,indent=6))
    else:
        print (json.dumps(_list,indent=6))
            

@click.command(help="get a json file")
@click.option("-i","--report-id",help="ScanReport ID on the website",required=True,type=int)
@click.option("-t","--token",help="specify the carrot_token for accessing the CCOM website",type=str,default=None)
@click.option("-u","--url",help="url endpoint for the CCOM website to ping",
              type=str,
              default="https://ccom.azurewebsites.net")
def _json(report_id,token,url):

    token = os.environ.get("carrot_TOKEN") or token
    if token == None:
        raise MissingToken("you must use the option --token or set the environment variable carrot_TOKEN to be able to use this functionality. I.e  export carrot_TOKEN=12345678 ")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        "Content-type": "application/json",
        "charset": "utf-8",
        "Authorization": f"Token {token}"
    }

    response = requests.get(
        f"{url}/api/json/?id={report_id}",
        headers=headers
    ).json()[0]

    print (json.dumps(response,indent=6))
    
@click.command(help="get a json file")
@click.argument('rules')
def object_list(rules):
    config = carrot.tools.load_json(rules)
    object_list = {
        dest: [k for k,v in rule_set.items()]
        for dest,rule_set in config['cdm'].items()
        }
    click.echo(json.dumps(object_list,indent=6))
    
get.add_command(object_list,"object-list-from-json")
get.add_command(_json,"json")
get.add_command(concepts,"concepts")
get.add_command(concept,"concept")
get.add_command(person_ids,"person-ids")
get.add_command(date_events,"date-events")
