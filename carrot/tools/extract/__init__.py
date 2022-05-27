import json
import pandas as pd
import os
from . import templates
import glob
from carrot.cdm import classes

def make_class(data,
               name=None,
               f_inputs=None):
               
    
    structural_mapping = data['cdm']

    metadata = data['metadata']
    person_ids = None
    if 'person_id' in metadata:
        person_ids = metadata['person_id']
        person_ids = { k.lower():v.lower() for k,v in person_ids.items() }

    
    objects = []
    for destination_table,_map in structural_mapping.items():
        for i,(_,obj) in enumerate(_map.items()):
            map_rules = []
            #find structural mapping

            for destination_field,source in sorted(obj.items()):
                source_field = source['source_field']
                source_table = source['source_table']
                rule = templates.rule.render(destination_field=destination_field,
                                             source_table=source_table,
                                             source_field=source_field)
                map_rules.append(rule)


            #add a line break
            map_rules.append('')
            map_rules.append('# --- insert field operations --- ')

            for destination_field,source in sorted(obj.items()):
                if 'operations' in source and source['operations'] is not None:
                    for operation in source['operations']:
                        rule = templates.operation.render(destination_field=destination_field,
                                                          operation=operation)
                        map_rules.append(rule)
                        
                
            #add a line break
            map_rules.append('')
            map_rules.append('# --- insert term mapping --- ')
            #find term mapping
            for destination_field,source in sorted(obj.items()):
                term_mapping = None
                if 'term_mapping' in source:
                    term_mapping = source['term_mapping']

                
                if term_mapping:
                    #term map each value
                    if isinstance(term_mapping,dict):
                        nindent=4
                        term_mapping = json.dumps(term_mapping,indent=nindent).splitlines()
                        temp = f'self.{destination_field}.series = self.{destination_field}.series.map('
                        map_rules.append(temp)
                        for line in term_mapping:
                            map_rules.append(f'{" "*nindent}{line}')
                        map_rules.append(f')')
                    #force all values to be a single value
                    else:
                        temp = f'self.{destination_field}.series = self.tools.make_scalar(self.{destination_field}.series,{term_mapping})'
                        map_rules.append(temp)
                        


            
            function_name = f"{destination_table}_{i}"
            objects.append(templates.obj.render(
                function_name=function_name,
                object_name=destination_table,
                map_rules=map_rules))


    init = templates.init.render(person_ids=person_ids)
    source_code = templates.cls.render(name=name, init=init, objects=objects)

    current_dir = os.getcwd()

    fname = os.path.join(current_dir, f"{name}.py")
    if os.path.isfile(fname):
        print (f"Recreating file {fname}")
    else:
        print (f"Making new file {fname}")
        
    with open(fname,'w') as f:
        f.write(source_code)

    return fname
    
def register_class(fname):
    name = os.path.basename(fname)
    #register the file within carrot/classes so it can be imported
    save_dir = os.path.dirname(os.path.abspath(classes.__file__))

    config_folder = os.environ.get('CONFIG_CONFIG_FOLDER')
    if config_folder is not None:
        save_dir = config_folder
        fname_dst = os.path.join(save_dir, name)
        if fname == fname_dst:
            print (f"File is already present in {config_folder}")
            return
        
    fname_dst = os.path.join(save_dir, name)
    if os.path.isfile(fname_dst):
        os.unlink(fname_dst)
    print (f"Registering file, creating a symlink to {fname_dst}")
    os.symlink(fname, fname_dst)

