import json
import pandas as pd
import os
from . import templates
import glob
from coconnect.cdm import classes

from coconnect.tools.logger import Logger
logger = Logger("extract")

def make_class(name,
               structural_mapping,
               f_inputs=None):
               

    #inputs = "test"#json.dumps({x.split("/")[-1]: x for x in sorted(glob.glob(f_inputs))},indent=6)


    objects = []
    for destination_table,_map in structural_mapping.items():
        for i,obj in enumerate(_map):
            map_rules = []
            #find structural mapping
            for destination_field,source in sorted(obj.items()):
                source_field = source['source_field']
                source_table = source['source_table']
                map_rules.append(f'self.{destination_field} = self.inputs["{source_table}"]["{source_field}"]')

            #add a line break
            map_rules.append('')
            map_rules.append('# --- insert term mapping --- ')
            #find term mapping
            for destination_field,source in sorted(obj.items()):
                term_mapping = source['term_mapping']
                    
                if term_mapping:
                    nindent=4
                    term_mapping = json.dumps(term_mapping,indent=nindent).splitlines()
                    temp = f'self.{destination_field} = self.{destination_field}.map('
                    map_rules.append(temp)
                    for line in term_mapping:
                        map_rules.append(f'{" "*nindent}{line}')
                    map_rules.append(f')')


            
            function_name = f"{destination_table}_{i}"
            objects.append(templates.obj.render(
                function_name=function_name,
                object_name=destination_table,
                map_rules=map_rules))

    #source_code = templates.cls.render(name='Panther', inputs=inputs, objects=objects)
    source_code = templates.cls.render(name=name, objects=objects)

    save_dir = os.path.dirname(os.path.abspath(classes.__file__))
    fname = f'{save_dir}/{name}.py'
    if os.path.isfile(fname):
        print (f"Recreating file {fname}")
    else:
        print (f"Making new file {fname}")
        
    with open(fname,'w') as f:
        f.write(source_code)
