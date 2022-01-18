import re
import requests
import inquirer
import json
from dotenv import dotenv_values
from pymongo import MongoClient
from pymongo import TEXT
client = MongoClient('localhost', port=27017)
client.server_info()

db = client.coconnect

mapped_concepts = db.mapped_concepts
#print (mapped_concepts.index_information().keys())
#mapped_concepts.drop_index('source_value_text_source_field_text_source_description_text_concept_name_text')
mapped_concepts.create_index([("source_value",TEXT), ("source_field",TEXT),("domain",TEXT),
                              ("source_description",TEXT),("concept_name",TEXT),
                              ("concept_id",TEXT)])
exit(0)

def find_concepts(source_value):
    concepts = mapped_concepts.aggregate([
        {'$match':{'source_value':source_value}},
        {'$project':{'_id':0,'concept_id':1,'concept_name':1}},
        {'$group':{'_id':{'concept_name':'$concept_name','concept_id':'$concept_id'}}},
        {"$project": {'_id': 0,'concept_id':'$_id.concept_id','concept_name': "$_id.concept_name",}}
    ])

    return list(concepts)

def search_for_concepts(term):
    concepts = mapped_concepts.aggregate([
        {'$match':{ '$text': { '$search': term } }},
        {'$group':{
            '_id':{'concept_name':'$concept_name','concept_id':'$concept_id','domain':'$domain'},
            'sources':{'$addToSet':{
                'source_data_partner':'$source_data_partner',
                'source_report':'$source_report',
                'source_table':'$source_table',
                'source_field':'$source_field',
                'source_value':'$source_value',
            }}
        }},
        {"$project": {
            '_id': 0,
            'concept_id':'$_id.concept_id',
            'concept_name':"$_id.concept_name",
            'domain':'$_id.domain',
            #'sources':'$sources',
        }}
    ])
    return list(concepts)

#print (find_concepts('M'))
print (json.dumps(search_for_concepts('problem'),indent=6))
exit(0)

config = dotenv_values(".env")
url = config['CCOM_URL']
token = config['CCOM_TOKEN']
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
    "Content-type": "application/json",
    "charset": "utf-8",
    "Authorization": f"Token {token}"
}

def get_field_and_table_name(scan_report_field_id):
    response = requests.get(
        f"{url}/api/scanreportfieldsfilter/?id={scan_report_field_id}&fields=name,scan_report_table",
        headers=headers
    )
    res = response.json()[0]
    name = res['name']

    report_id = res['scan_report_table']
    response = requests.get(
        f"{url}/api/scanreporttablesfilter/?id={report_id}&fields=name",
        headers=headers
    )
    res = response.json()
    table = res[0]['name']
    
    return name,table
    


def get_values(field_id):
    response = requests.get(
            f"{url}/api/scanreportvaluesfilter/?scan_report_field={field_id}&fields=id,value,value_description",
        headers=headers
    )
    return response.json()

def get_concept(concept_id):
    response = requests.get(
            f"{url}/api/omop/concepts/{concept_id}",
            headers=headers
    )
    return response.json()

def insert_concept_id(value_id,concept_id):
    post_data = {
        'concept': int(concept_id),
        'object_id': int(value_id),
        'content_type': 17
    }
    response = requests.post(
        f"{url}/api/scanreportconcepts/",
        data=json.dumps(post_data),
        headers=headers
    )
    print (response.json())

response = requests.get(
    f"{url}/api/scanreporttablesfilter/?scan_report=317&fields=id,name",
    headers=headers
)
tables = response.json()

def get_fields(table_id):
    response = requests.get(
        f"{url}/api/scanreportfieldsfilter/?scan_report_table={table_id}&fields=id,name",
        headers=headers
    )
    fields = response.json()
    return fields

questions = [
    inquirer.Checkbox('tables',
                      message=f"Which tables to process?",
                      choices=tables)
                      

]
answers = inquirer.prompt(questions)

tables = answers['tables']

for obj in tables:
    table_id = obj['id']
    fields = get_fields(table_id)
    
    questions = [
        inquirer.Checkbox('fields',
                          message=f"{obj['name']}: Which fields to process?",
                          choices=fields)        
    ]
    answers = inquirer.prompt(questions)
    fields = answers['fields']

    for field in fields:
        field_id = field['id']
        values = get_values(field_id)
        
        questions = [
            inquirer.Checkbox('values',
                          message=f"{field['name']}: Which values to insert concepts for? ",
                          choices=values)
        ]
        answers = inquirer.prompt(questions)
        values = answers['values']
        
        for obj in values:
            value = obj['value']
            concepts = find_concepts(value)
            
            concepts.append("Manually insert")
            concepts.append("None of the above")
            questions = [
                inquirer.List('concepts',
                              message=f"For source_value '{value}' .. Use one of these concepts? ",
                              choices=concepts)
                
            ]
            answers = inquirer.prompt(questions)

            if answers['concepts'] == "None of the above":
                continue
            elif answers['concepts'] == "Manually insert":
                questions = [
                    inquirer.Text(name='concept_id', message="Which concept do you want to use?",validate=lambda _, x : int(x))
                ]
                answers = inquirer.prompt(questions)
                concept_id = answers['concept_id']

                concept = get_concept(concept_id)
                concept_name = concept["concept_name"]
                print (json.dumps(concept,indent=6))
                correct = inquirer.confirm(f"Sure you want to add {concept_id} ({concept_name})", default=True)
                if not correct:
                    continue
                
            else:
                concept_id = answers['concepts']['concept_id']

            value_id = obj['id']
            insert_concept_id(value_id,concept_id)
