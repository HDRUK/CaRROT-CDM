import carrot.tools as tools

class OmopCDM:

    def __init__(self, omopcfg):
        self.omop_json = tools.load_json(omopcfg)
        self.all_columns = self.get_columns("all_columns")
        self.date_fields = self.get_columns("date_fields")
        self.date_field_components = self.get_columns("date_field_components")
        self.datetime_fields = self.get_columns("datetime_fields")
        self.person_id_field = self.get_columns("person_id_field")
        self.auto_number_field = self.get_columns("auto_number_field")

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
        if fieldname in self.get_omop_date_fields(tablename):
            return False
        if fieldname in self.get_omop_datetime_fields(tablename):
            return False
        if fieldname in self.get_omop_person_id_field(tablename):
            return False
        return True

    def get_omop_date_fields(self, tablename):
        if self.date_fields != None:
            if tablename in self.date_fields:
                return self.date_fields[tablename]
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
        return None

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
