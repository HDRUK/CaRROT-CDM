from coconnect.cdm import CommonDataModel
from coconnect.cdm import mapping_pipeline_helpers
import json
import pandas as pd

from . import templates


import glob

def make_class(name='panther',
               f_sm='PANTHER/University of Nottingham_PANTHER ds_structural_mapping.csv',
               f_inputs='PANTHER/synthetic_data/*.csv',
               tables=['person','condition_occurrence']):
               

    inputs = json.dumps({x.split("/")[-1]: x for x in sorted(glob.glob(f_inputs))},indent=6)
    
    structural_mapping = mapping_pipeline_helpers.StructuralMapping\
                                                 .to_json(f_sm,
                                                          destination_tables = tables,
                                                          save=f'{name}_structural_mapping.json')
    print (json.dumps(structural_mapping,indent=6))

    objects = []
    for destination_table,_map in structural_mapping.items():
        for i,obj in enumerate(_map):
            map_rules = []
            for destination_field,source in obj.items():
                source_field = source['source_field']
                source_table = source['source_table']
                map_rules.append(f'self.{destination_field} = self.inputs["{source_table}"]["{source_field}"]')
            function_name = f"{destination_table}_{i}"
            objects.append(templates.obj.render(
                function_name=function_name,
                object_name=destination_table,
                map_rules=map_rules))

    source_code = templates.cls.render(name='Panther', inputs=inputs, objects=objects)
    print (source_code)
    
    with open(f'{name}_dynamics.py','w') as f:
        f.write(source_code)
