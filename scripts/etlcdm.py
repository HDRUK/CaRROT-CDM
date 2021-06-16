import argparse
import json
from coconnect.cdm import (
    CommonDataModel,
    get_cdm_class
)

from coconnect.cdm import (
    Person,
    ConditionOccurrence,
    Measurement,
    Observation
)

from coconnect.tools import (
    load_csv,
    load_json,
    apply_rules
)

class TableNotFoundError(Exception):
    pass
class FieldNotFoundError(Exception):
    pass

def main():
    parser = argparse.ArgumentParser(description='ETL-CDM: transform a dataset into a CommonDataModel.')
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

    config = load_json(args.rules)

    inputs = load_csv(
        {
            x.split("/")[-1].lower():x
            for x in args.inputs
        }
    )

    name = config['metadata']['dataset']

    #build an object to store the cdm
    cdm = CommonDataModel(name=name,
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
            # destination_table : person
            # get_cdm_class returns <Person>
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


