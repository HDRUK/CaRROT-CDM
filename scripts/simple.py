import argparse
import json
from coconnect.cdm import CommonDataModel, Person, ConditionOccurrence, Measurement, Observation
from coconnect.tools import load_csv

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

    cdm = CommonDataModel(name='test',inputs=inputs,output_folder=args.out_dir)
    
    for destination_table,rules_set in config['cdm'].items():
        for i,rules in enumerate(rules_set):
            if destination_table == 'person':
                obj = Person()
            elif destination_table == 'measurement':
                obj = Measurement()
            elif destination_table == 'observation':
                obj = Observation()
            elif destination_table == 'condition_occurrence':
                obj = ConditionOccurrence()
            else:
                raise NotImplemented(f"{destination_table}")

            for destination_field,rule in rules.items():
                source_table = rule['source_table']
                source_field = rule['source_field']
                operations = rule['operations']
                term_mapping = rule['term_mapping']

                series = inputs[source_table][source_field].copy()
                
                if operations is not None:
                    for operation in operations:
                        function = cdm.tools[operation]
                        series = function(series)
                        
                if term_mapping is not None:

                    if isinstance(term_mapping,dict):
                        series = series.map(term_mapping)
                    else:
                        series.values[:] = term_mapping

                setattr(obj,destination_field,series)
                
            setattr(cdm,f"{destination_table}_{i}",obj)
                
    cdm.process()

if __name__ == "__main__":
    main()


