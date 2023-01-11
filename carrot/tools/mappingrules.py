import os
import json
from .omopcdm import OmopCDM

class MappingRules:

    def __init__(self, rulesfilepath):
        self.rules_data = self.load_json(rulesfilepath)
        self.omopcdm = OmopCDM()

    def load_json(self, f_in):
        """
        """
        if os.path.exists(f_in):
            data = json.load(open(f_in))
        else:
            try:
                data = json.loads(f_in)
            except Exception as err:
                raise FileNotFoundError(f"{f_in} not found. Or cannot parse as json")

        return data

    def get_all_outfile_names(self):
        file_list = []
        for outfilename in self.rules_data["cdm"]:
            file_list.append(outfilename)

        return file_list

    def parse_rules_src_to_tgt(self, infilename):
        """
        Parse rules to produce a map of source to target data for a given input file
        """
        outfilenames = []
        outdata = {}

        for outfilename, rules_set in self.rules_data["cdm"].items():
            for datatype, rules in rules_set.items():
                key, data = self.process_rules(infilename, outfilename, rules)
                if key != "":
                    if key not in outdata:
                        outdata[key] = []
                    outdata[key].append(data)
            if outfilename not in outfilenames:
                outfilenames.append(outfilename)

        return outfilenames, outdata

    def process_rules(self, infilename, outfilename, rules):
        outkey = ""
        data = {}
        plain_key = ""
        term_value_key = ""

        for outfield, source_info in rules.items():
            if source_info["source_field"] not in data:
                data[source_info["source_field"]] = []
            if source_info["source_table"] == infilename:
                if "term_mapping" in source_info:
                    if type(source_info["term_mapping"]) is dict:
                        for inputvalue, term in source_info["term_mapping"].items():
                            term_value_key = infilename + "~" + source_info["source_field"] + "~" + str(inputvalue) + "~" + outfilename
                            data[source_info["source_field"]].append(outfield + "~" + str(source_info["term_mapping"][str(inputvalue)]))
                    else:
                        plain_key = infilename + "~" + source_info["source_field"] + "~" + outfilename
                        data[source_info["source_field"]].append(outfield + "~" + str(source_info["term_mapping"]))
                else:
                    data[source_info["source_field"]].append(outfield)
        if term_value_key != "":
            return term_value_key, data

        return plain_key, data
