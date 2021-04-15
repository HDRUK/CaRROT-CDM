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
                f_pk_mapping=None,
                filter_destination_tables=None,
                filter_source_tables=None,
                for_synthetic=False,
                strict=True,
                save=None,
                **kwargs):

        _metadata = kwargs
        self.df_structural_mapping = pd.read_json(f_structural_mapping)
        if filter_destination_tables:
            self.df_structural_mapping = self.df_structural_mapping[
                self.df_structural_mapping['destination_table'].isin(filter_destination_tables)
            ]
 
        if filter_source_tables:
            self.df_structural_mapping = self.df_structural_mapping[
                self.df_structural_mapping['source_table'].isin(filter_source_tables)
            ]
            
            
        
        if isinstance(f_pk_mapping,str):
            pk_mapping = json.load(open(f_pk_mapping))
        elif f_pk_mapping:
            pk_mapping = json.load(f_pk_mapping)
            
        self.df_structural_mapping.set_index('destination_table',inplace=True)
        destination_tables = self.df_structural_mapping.index.unique()

        
        _map = {}
        #loop over all destination tables
        for destination_table in destination_tables:

            _map[destination_table] = []

            #find all rules associated with that destination table
            all_rules = self.df_structural_mapping.loc[destination_table]
            if isinstance(all_rules,pd.core.series.Series):
                all_rules = all_rules.to_frame().T

            #do some indexing to set the source table as the new index
            all_rules.set_index('source_table',inplace=True)
            #find all source tables associated with this destination_table
            source_tables = all_rules.index.unique()

            #loop over the source tables
            for source_table in source_tables:
                rules = all_rules.loc[source_table]
                
                #count how many times a rule appears for a destination field and a source field
                values = rules['destination_field'].value_counts()
                unique_values = sorted(values.unique())

                
                
                if len(unique_values) > 2:
                    print ('having to skip',destination_table)
                    if strict:
                        raise MissConfiguredStructuralMapping("something really wrong")
                    else:
                        continue

                # get all the base fields, rules in which there are
                #    one rule per source and destination field
                #    these will be person_id, and dates
                #    e.g. condition_occurrence::person_id
                # concept_ids are the ones that can have values>1
                #    you might have multiple conditions in a field value,
                #    all with the same person_id and datetime event
                #    e.g condition_occurrence::condition_concept_id


                # in this example scenario, the unique values look like this:
                #condition_source_value         9
                #condition_concept_id           9
                #condition_source_concept_id    9
                #condition_start_date           1
                #person_id                      1

                #this means that there are 9 conditions in a given table & field
                #and we only have one table & field for the person_id and start_date
                #the latter (values==1) will be used as the base
                #then we'll loop over the rest (values==9)
                #and copy person_id, condition_start_date from the base when building
                
                
                base_fields = sorted(values[values==1].index)

                                
                _dmap = {}

                rules = rules.reset_index().set_index('destination_field')
                
                
                #build rules from the base_fields
                for destination_field in base_fields:
                    rule = rules.loc[destination_field]
                    source_table = rule['source_table'].lower()
                    source_field = rule['source_field'].lower()
                    term_mapping = rule['term_mapping']
                    operations = rule['operations']
                    obj = {
                        'source_table':source_table,
                        'source_field':source_field,
                        'operations':operations,
                        'term_mapping':term_mapping
                    }
                    _dmap[destination_field] = obj

              
                #save these base rules
                _map[destination_table].append(_dmap)

                #now move onto multiple rules that need to use the base
                if len(unique_values) == 2:
                    #extract the non-base rules
                    duplicates = values[values==unique_values[1]].index
                    #loop over them
                    for i,destination_field in enumerate(duplicates):
                        #ordering of these is going funny..
                        #temp fix is to order by source_field so they are in line with each other
                        rule = rules.loc[destination_field].sort_values('source_field')
                        source_tables = rule['source_table'].str.lower()
                        source_fields = rule['source_field'].str.lower()
                        term_mappings = rule['term_mapping']
                        operationss = rule['operations']
 
 
                        for j,(source_table,source_field,term_mapping,operations)\
                            in enumerate(zip(source_tables,source_fields,term_mappings,operationss)):
                            obj = {
                                'source_table':source_table,
                                'source_field':source_field,
                                'operations':operations,
                                'term_mapping':term_mapping
                            }
                            

                            if i == 0 and j>0:
                                _dmap = copy.copy(_map[destination_table][0])
                                _dmap[destination_field] = obj
                                _map[destination_table].append(_dmap)
                            else:
                                _map[destination_table][j][destination_field] = obj



        #print (json.dumps(_map,indent=6))
                            
        if 'person' in _map and len(_map['person']) > 1:
            print (json.dumps(_map['person'],indent=6))
            raise MultiplePersonDefined("Something wrong, more than one person object is being defined"
                                        "Likely because a mapping has been defined twice")

        _metadata.update(
            {
                'date_created':datetime.utcnow().isoformat()
            })


        all_used_tables = self.df_structural_mapping.loc[destination_tables]['source_table'].unique()

        #missing = list((set(all_used_tables) - set(pk_mapping.keys())))
        #if len(missing)>0:
        #    raise NoPrimaryKeyDefined(f"you use {missing} without defining which field is the pk (person id)")

        #_metadata.update(
        #    {
        #        'person_id':{k:v for k,v in sorted(pk_mapping.items()) }
        #    })
        
        _output = {'metadata':_metadata,'cdm':_map}
        if not save is None:
            json.dump(_output,open(save,'w'),indent=6)
        return _output
