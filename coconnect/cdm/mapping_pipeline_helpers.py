import copy
import json
import pandas as pd

class MissConfiguredStructuralMapping(Exception):
    pass

class StructuralMapping:

    @classmethod
    def to_json(self,f_structural_mapping,destination_tables=None,for_synthetic=False,save=None):
        self.df_structural_mapping = pd.read_csv(f_structural_mapping)
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
            values = rules['destination_field'].value_counts()
            unique_values = sorted(values.unique())
            if len(unique_values) > 2:
                print (values)
                raise MissConfiguredStructuralMapping("something really wrong")

            rules.set_index('destination_field',inplace=True)
            initial = values[values==1].index

            _dmap = {}
            for destination_field in initial:
                rule = rules.loc[destination_field]
                source_table = rule['source_table'].lower()
                source_field = rule['source_field'].lower()
                obj = {'source_table':source_table,
                       'source_field':source_field}
                _dmap[destination_field] = obj

            _map[destination_table].append(_dmap)

            if len(unique_values) == 2:
                duplicates = values[values==unique_values[1]].index
                for i,destination_field in enumerate(duplicates):

                    rule = rules.loc[destination_field]
                    source_tables = rule['source_table'].str.lower()
                    source_fields = rule['source_field'].str.lower()

                    
                    for j,(source_table,source_field) in enumerate(zip(source_tables,source_fields)):
                        obj = {'source_table':source_table,
                               'source_field':source_field}

                        if i == 0 and j>0:
                            _dmap = copy.copy(_map[destination_table][0])
                            _dmap[destination_field] = obj
                            _map[destination_table].append(_dmap)
                        else:
                            _map[destination_table][j][destination_field] = obj

        if not save is None:
            json.dump(_map,open(save,'w'),indent=6)
        return _map
