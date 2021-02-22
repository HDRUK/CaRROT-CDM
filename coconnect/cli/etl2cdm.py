#!/usr/bin/env python

from coconnect import ETLTool

import argparse


parser = argparse.ArgumentParser(description='Tool for mapping datasets')
parser.add_argument('--inputs','-i', nargs='+', required=True,
                    help='input .csv files for the original data to be mapped to the CDM')
parser.add_argument('--output-folder','-o', default=None,
                    help='location of where to store the data')
parser.add_argument('--term-mapping','-tm', required=False, default=None,
                    help='file that will handle the term mapping')
parser.add_argument('--structural-mapping','-sm', required=True,
                    help='file that will handle the structural mapping')
parser.add_argument('--chunk-size', default = None, type=int,
                    help='define how to "chunk" the dataframes, this specifies how many rows in the csv files to read in at a time')
parser.add_argument('-v','--verbose',help='set debugging level',action='store_true')
parser.add_argument('--mask-id',type=int,choices=[0,1],help='masking of the patient id',default=1)
parser.add_argument('--auto-map',type=int,choices=[0,1],help='allow automatically mapping unmapped fields',default=1)


def main():
    args = parser.parse_args()

    runner = ETLTool()

    runner.set_verbose(args.verbose)
    if args.chunk_size != None:
        runner.set_chunk_size(args.chunk_size)
    if args.output_folder != None:
        runner.set_output_folder(args.output_folder)
       
    runner.set_perform_person_id_mask(args.mask_id)
    runner.set_use_auto_functions(args.auto_map)
        
    runner.load_input_data(args.inputs)
    runner.load_structural_mapping(args.structural_mapping)
    if args.term_mapping != None:
        runner.load_term_mapping(args.term_mapping)

    runner.run()

        
if __name__ == '__main__':
    main()
