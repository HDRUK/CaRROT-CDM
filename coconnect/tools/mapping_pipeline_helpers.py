import copy
import json
import pandas as pd
import numpy as np
from datetime import datetime

class MultiplePersonDefined(Exception):
    pass

class MissConfiguredStructuralMapping(Exception):
    pass

class NoPrimaryKeyDefined(Exception):
    pass

class StructuralMapping:
    @classmethod
    def to_json(self,
                f_structural_mapping,
                f_pk_mapping,
                destination_tables=None,
                for_synthetic=False,
                strict=True,
                save=None,
                **kwargs):

        _metadata = kwargs
        self.df_structural_mapping = pd.read_json(f_structural_mapping)
        
        if isinstance(f_pk_mapping,str):
            pk_mapping = json.load(open(f_pk_mapping))
        else:
            pk_mapping = json.load(f_pk_mapping)
            
        if for_synthetic:
            for col in ['source_table','source_field']:
                self.df_structural_mapping[col] = self.df_structural_mapping[col].str.lower()
        
        self.df_structural_mapping.set_index('destination_table',inplace=True)
        if destination_tables == None:
            destination_tables = self.df_structural_mapping.index.unique()

        _map = {}
        
        for destination_table in destination_tables:
            
            _map[destination_table] = []

            rules = self.df_structural_mapping.loc[destination_table]
            if isinstance(rules,pd.core.series.Series):
                rules = rules.to_frame().T


            values = rules['destination_field'].value_counts()
            unique_values = sorted(values.unique())
            if len(unique_values) > 2:
                print (values)
                print ('having to skip',destination_table)
                if strict:
                    raise MissConfiguredStructuralMapping("something really wrong")
                else:
                    continue

            rules.set_index('destination_field',inplace=True)

            initial = sorted(values[values==1].index)

            _dmap = {}
            for destination_field in initial:
                rule = rules.loc[destination_field]
                source_table = rule['source_table'].lower()
                source_field = rule['source_field'].lower()
                term_mapping = rule['term_mapping']
                
                obj = {
                    'source_table':source_table,
                    'source_field':source_field,
                    'term_mapping':term_mapping
                }
                _dmap[destination_field] = obj

            _map[destination_table].append(_dmap)

            if len(unique_values) == 2:
                duplicates = values[values==unique_values[1]].index
                for i,destination_field in enumerate(duplicates):

                    rule = rules.loc[destination_field]
                    source_tables = rule['source_table'].str.lower()
                    source_fields = rule['source_field'].str.lower()
                    term_mappings = rule['term_mapping']

                    
                    for j,(source_table,source_field,term_mapping)\
                        in enumerate(zip(source_tables,source_fields,term_mappings)):
                        obj = {
                            'source_table':source_table,
                            'source_field':source_field,
                            'term_mapping':term_mapping
                        }

                        if i == 0 and j>0:
                            _dmap = copy.copy(_map[destination_table][0])
                            _dmap[destination_field] = obj
                            _map[destination_table].append(_dmap)
                        else:
                            _map[destination_table][j][destination_field] = obj


                            
        if 'person' in _map and len(_map['person']) > 1:
            print (json.dumps(_map['person'],indent=6))
            raise MultiplePersonDefined("Something wrong, more than one person object is being defined"
                                        "Likely because a mapping has been defined twice")

        _metadata.update(
            {
                'date_created':datetime.utcnow().isoformat()
            })


        all_used_tables = self.df_structural_mapping.loc[destination_tables]['source_table'].unique()

        missing = list((set(all_used_tables) - set(pk_mapping.keys())))
        if len(missing)>0:
            raise NoPrimaryKeyDefined(f"you use {missing} without defining which field is the pk (person id)")

        _metadata.update(
            {
                'person_id':{k:v for k,v in sorted(pk_mapping.items()) }
            })
        
        
        _output = {'metadata':_metadata,'cdm':_map}
        if not save is None:
            json.dump(_output,open(save,'w'),indent=6)
        return _output
