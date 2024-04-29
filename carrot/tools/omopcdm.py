import carrot.tools as tools
import json
import re

class OmopCDM:

    def __init__(self, omopddl, omopcfg):
        self.numeric_types = ["integer", "numeric"]
        self.datetime_types = ["timestamp"]
        self.date_types = ["date"]
        self.omop_json = self.load_ddl(omopddl)
        self.omop_json = self.merge_json(self.omop_json, omopcfg)
        self.all_columns = self.get_columns("all_columns")
        self.numeric_fields = self.get_columns("numeric_fields")
        self.notnull_numeric_fields = self.get_columns("notnull_numeric_fields")
        self.datetime_linked_fields = self.get_columns("datetime_linked_fields")
        self.date_field_components = self.get_columns("date_field_components")
        self.datetime_fields = self.get_columns("datetime_fields")
        self.person_id_field = self.get_columns("person_id_field")
        self.auto_number_field = self.get_columns("auto_number_field")


    def load_ddl(self, omopddl):
        try:
            fp = open(omopddl, "r") 
        except IOError as e:
            print("I/O error for ddl file ({0}): {1}".format(e.errno, e.strerror))
            exit()
        
        return(self.process_ddl(fp))

    def process_ddl(self, fp):
        """
        Process the omop ddl file to output the attributes which CaRROT-CDM understands
        Matching of selected parts of the ddl definition is performed using rgx's
        """
        output_dict = {}
        output_dict["all_columns"] = {}
        output_dict["numeric_fields"] = {}
        output_dict["notnull_numeric_fields"] = {}
        output_dict["datetime_fields"] = {}
        output_dict["date_fields"] = {}

        ver_rgx = re.compile(r'^--postgresql.*(\d+\.\d+)$')
        start_rgx = re.compile(r'^CREATE\s*TABLE\s*(\@?[a-zA-Z]+\.)?([A-Z_]+)')
        datatype_rgx = re.compile(r'^\s*([a-z_]+)\s+([a-zA-Z_]+)')
        end_rgx = re.compile(r'.*[)];$')
        vermatched = False
        processing_table_data = False
        tabname = ""

        for line in fp:
            line = line.strip()
            # check for line with version, if present
            if vermatched == False:
                vmatch = ver_rgx.search(line)
                if vmatch != None:
                    version_string = vmatch.group(1)
                    output_dict["omop_version"] = version_string
                    vermatched = True
            # check for start of table definition
            if processing_table_data == False:
                smatch = start_rgx.search(line)
                if smatch != None:
                    processing_table_data = True
                    tabname = smatch.group(2).lower()
            else:
                idtmatch = datatype_rgx.search(line)
                if idtmatch != None:
                    fname = idtmatch.group(1)
                    ftype = idtmatch.group(2)

                    # Check for dictionary element presence
                    if tabname not in output_dict["all_columns"]:
                        output_dict["all_columns"][tabname] = []
                    if tabname not in output_dict["numeric_fields"]:
                        output_dict["numeric_fields"][tabname] = []
                    if tabname not in output_dict["notnull_numeric_fields"]:
                        output_dict["notnull_numeric_fields"][tabname] = []
                    if tabname not in output_dict["datetime_fields"]:
                        output_dict["datetime_fields"][tabname] = []
                    if tabname not in output_dict["date_fields"]:
                        output_dict["date_fields"][tabname] = []
  
                    # Add in required column / field data
                    output_dict["all_columns"][tabname].append(fname)
                    if ftype.lower() in self.numeric_types:
                        output_dict["numeric_fields"][tabname].append(fname)
                    if ftype.lower() in self.numeric_types and "NOT" in line and "NULL" in line:
                        output_dict["notnull_numeric_fields"][tabname].append(fname)
                    if ftype.lower() in self.datetime_types:
                        output_dict["datetime_fields"][tabname].append(fname)
                    if ftype.lower() in self.date_types:
                        output_dict["date_fields"][tabname].append(fname)

            ematch = end_rgx.search(line)
            if ematch != None:
                processing_table_data = False
  
        return(output_dict)
    
    def dump_ddl(self):
        return(json.dumps(self.omop_json, indent=2))

    def merge_json(self, omopjson, omopcfg):
        tmp_json = tools.load_json(omopcfg)
        for key, data in tmp_json.items():
            omopjson[key] = data
        return(omopjson)

    def get_columns(self, colkey):
        if colkey in self.omop_json:
            return self.omop_json[colkey]
        return None

    def get_column_map(self, colarr, delim=","):
        colmap = {}
        for i, col in enumerate(colarr):
            colmap[col] = i
        return colmap

    def get_omop_column_map(self, tablename):
        if tablename in self.all_columns:
            return self.get_column_map(self.all_columns[tablename])
        return None

    def get_omop_column_list(self, tablename):
        if tablename in self.all_columns:
            return self.all_columns[tablename]
        return None

    def is_omop_data_field(self, tablename, fieldname):
        if fieldname in self.get_omop_datetime_linked_fields(tablename):
            return False
        if fieldname in self.get_omop_datetime_fields(tablename):
            return False
        if fieldname in self.get_omop_person_id_field(tablename):
            return False
        return True

    def get_omop_numeric_fields(self, tablename):
        if self.numeric_fields != None:
            if tablename in self.numeric_fields:
                return self.numeric_fields[tablename]
        return []

    def get_omop_notnull_numeric_fields(self, tablename):
        if self.notnull_numeric_fields != None:
            if tablename in self.notnull_numeric_fields:
                return self.notnull_numeric_fields[tablename]
        return []

    def get_omop_datetime_linked_fields(self, tablename):
        if self.datetime_linked_fields != None:
            if tablename in self.datetime_linked_fields:
                return self.datetime_linked_fields[tablename]
        return {}

    def get_omop_date_field_components(self, tablename):
        if self.date_field_components != None:
            if tablename in self.date_field_components:
                return self.date_field_components[tablename]
        return {}

    def get_omop_datetime_fields(self, tablename):
        if self.datetime_fields != None:
            if tablename in self.datetime_fields:
                return self.datetime_fields[tablename]
        return []

    def get_omop_person_id_field(self, tablename):
        if self.person_id_field != None:
            if tablename in self.person_id_field:
                return self.person_id_field[tablename]
        return None

    def get_omop_auto_number_field(self, tablename):
        if self.auto_number_field != None:
            if tablename in self.auto_number_field:
                return self.auto_number_field[tablename]
        return None
