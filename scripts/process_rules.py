import argparse
import json
from coconnect.cdm import CommonDataModel, get_cdm_class
from coconnect.cdm import Person, ConditionOccurrence, Measurement, Observation
from coconnect.tools import load_csv

class TableNotFoundError(Exception):
    pass
class FieldNotFoundError(Exception):
    pass

def get_source_field(table,name):
    if name not in table:
        if name.lower() in table:
            return table[name.lower()]
        else:
            raise FieldNotFoundError(f"Cannot find {name} in table {table.name}. Options are {table.columns.tolist()}")
    return table[name]

def get_source_table(inputs,name):
    #make a copy of the input data column slice
    if name not in inputs.keys():
        short_keys = {key[:31]:key for key in inputs.keys()}
        if name in short_keys:
            name = short_keys[name]
        elif name.lower() in short_keys:
            name = short_keys[name.lower()]
        else:
            raise TableNotFoundError(f"Cannot find {name} in inputs. Options are {inputs.keys()}")
    inputs[name].name = name
    return inputs[name]



def apply_rules(cdm,obj,rules):
    for destination_field,rule in rules.items():
        source_table_name = rule['source_table']
        source_field_name = rule['source_field']
        operations = None
        if 'operations' in rule:
            operations = rule['operations']
        term_mapping = None
        if 'term_mapping' in rule:
            term_mapping = rule['term_mapping']


        source_table = get_source_table(cdm.inputs,source_table_name)
        source_field = get_source_field(source_table,source_field_name)
        series = source_field.copy()

        if operations is not None:
            for operation in operations:
                function = cdm.tools[operation]
                series = function(series)
                
        if term_mapping is not None:
            if isinstance(term_mapping,dict):
                # value level mapping
                # - term_mapping is a dictionary between values and concepts
                # - map values in the input data, based on this map
                series = series.map(term_mapping)
            else:
                # field level mapping.
                # - term_mapping is the concept_id
                # - set all values in this column to it
                series.values[:] = term_mapping

        obj[destination_field].series = series


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--rules', dest='rules',
                        required=True,
                        help='input .json file')
    parser.add_argument('--out-dir','-o', dest='out_dir',
                        required=True,
                        help='name of the output folder')
    parser.add_argument('--inputs','-i', dest='inputs',
                        required=True,
                        nargs="+",
                        help='input csv files')

    args = parser.parse_args()

    with open(args.rules) as rules_file:
        config = json.load(rules_file)
    
    inputs = load_csv(
        {
            x.split("/")[-1].lower():x
            for x in args.inputs
        }
    )

    name = config['metadata']['dataset']

    #build an object to store the cdm
    cdm = CommonDataModel(name='test',
                          inputs=inputs,
                          output_folder=args.out_dir)

    #loop over the cdm object types defined in the configuration
    #e.g person, measurement etc..
    for destination_table,rules_set in config['cdm'].items():
        #loop over each object instance in the rule set
        #for example, condition_occurrence may have multiple rulesx
        #for multiple condition_ocurrences e.g. Headache, Fever ..
        for i,rules in enumerate(rules_set):
            #make a new object for the cdm object
            #Example:
            # destination_field : person
            # obj : Person()
            obj = get_cdm_class(destination_table)()
            #set the name of the object
            obj.set_name(f"{destination_table}_{i}")
            #call the apply_rules function to setup how to modify the inputs
            #based on the rules
            apply_rules(cdm,obj,rules)
            #register this object with the CDM model, so it can be processed
            cdm.add(obj)
    cdm.process()
    print ('Finished Producing',cdm.keys())
    
if __name__ == "__main__":
    main()


