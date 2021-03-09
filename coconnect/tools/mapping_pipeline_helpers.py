import copy
import json
import pandas as pd
import numpy as np

class MissConfiguredStructuralMapping(Exception):
    pass

class StructuralMapping:
    @classmethod
    def to_json(self,f_structural_mapping,f_term_mapping=None,destination_tables=None,for_synthetic=False,save=None):
        self.df_structural_mapping = pd.read_csv(f_structural_mapping)

        if for_synthetic:
            for col in ['source_table','source_field']:
                self.df_structural_mapping[col] = self.df_structural_mapping[col].str.lower()
        
        self.df_structural_mapping.set_index('destination_table',inplace=True)
        if destination_tables == None:
            destination_tables = self.df_structural_mapping.index.unique()

        if f_term_mapping is not None:
            self.df_term_mapping = pd.read_csv(f_term_mapping)
            self.df_term_mapping = self.df_term_mapping\
                                       .groupby('rule_id')\
                                       .apply(lambda x: \
                                              {
                                                  k:v
                                                  for k, v in zip(x['source_term'], x['destination_term'])
                                              })\
                                       .reset_index()\
                                       .set_index('rule_id')
            self.df_term_mapping.columns = ['term_map']

            
        _map = {}
        
        for destination_table in destination_tables:
            
            _map[destination_table] = []

            rules = self.df_structural_mapping.loc[destination_table]
            values = rules['destination_field'].value_counts()
            unique_values = sorted(values.unique())
            if len(unique_values) > 2:
                print (values)
                raise MissConfiguredStructuralMapping("something really wrong")

            rules.set_index('destination_field',inplace=True)
            
            rules = rules.reset_index().set_index('rule_id')\
                                       .join(self.df_term_mapping)\
                                       .set_index('destination_field')\
                                       .replace({np.NaN:None})
    

            initial = values[values==1].index

            _dmap = {}
            for destination_field in initial:
                rule = rules.loc[destination_field]
                source_table = rule['source_table'].lower()
                source_field = rule['source_field'].lower()
                term_mapping = rule['term_map']
                
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
                    term_mappings = rule['term_map']

                    
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

        if not save is None:
            json.dump(_map,open(save,'w'),indent=6)
        return _map
