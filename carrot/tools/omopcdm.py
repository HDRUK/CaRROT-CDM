class OmopCDM:

    def __init__(self):
        self.all_columns = {
                         "condition_occurrence": ["condition_occurrence_id", "person_id",
                                                  "condition_concept_id", "condition_start_date",
                                                  "condition_start_datetime",    "condition_end_date",
                                                  "condition_end_datetime", "condition_type_concept_id",
                                                  "stop_reason", "provider_id", "visit_occurrence_id",
                                                  "condition_source_value", "condition_source_concept_id",
                                                  "condition_status_source_value", "condition_status_concept_id"],
                         "death": ["person_id", "death_date", "death_datetime", "death_type_concept_id",
                                   "cause_concept_id", "cause_source_value", "cause_source_concept_id"],
                         "drug_exposure": ["drug_exposure_id", "person_id", "drug_concept_id",
                                           "drug_exposure_start_date", "drug_exposure_start_datetime",
                                           "drug_exposure_end_date", "drug_exposure_end_datetime", "verbatim_end_date",
                                           "drug_type_concept_id", "stop_reason",
                                           "refills", "quantity", "days_supply", "sig", "route_concept_id",
                                           "lot_number", "provider_id", "visit_occurrence_id",
                                           "drug_source_value", "drug_source_concept_id", "route_source_value",
                                           "dose_unit_source_value"],
                         "measurement": ["measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_datetime", "measurement_type_concept_id",
                                         "operator_concept_id", "value_as_number", "value_as_concept_id", "unit_concept_id", "range_low", "range_high", "provider_id",
                                         "visit_occurrence_id", "measurement_source_value", "measurement_source_concept_id", "unit_source_value", "value_source_value"],
                         "observation": ["observation_id", "person_id", "observation_concept_id", "observation_date",
                                         "observation_datetime", "observation_type_concept_id", "value_as_number",
                                         "value_as_string", "value_as_concept_id", "qualifier_concept_id", "unit_concept_id provider_id",
                                         "visit_occurrence_id", "observation_source_value", "observation_source_concept_id", "unit_source_value", "qualifier_source_value"],
                         "person": ["person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime", "race_concept_id", "ethnicity_concept_id",
                                    "location_id", "provider_id", "care_site_id", "person_source_value", "gender_source_value", "gender_source_concept_id",
                                    "race_source_value", "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"],
                         "procedure_occurrence": ["procedure_occurrence_id", "person_id",            "procedure_concept_id", "procedure_date",     "procedure_datetime", "procedure_type_concept_id", "modifier_concept_id",
                                                  "quantity", "provider_id", "visit_occurrence_id", "procedure_source_value", "procedure_source_concept_id", "qualifier_source_value"],
                         "specimen": ["specimen_id", "person_id", "specimen_concept_id", "specimen_type_concept_id", "specimen_date", "specimen_datetime", "quantity", "unit_concept_id",
                                      "anatomic_site_concept_id", "disease_status_concept_id", "specimen_source_id", "specimen_source_value", "unit_source_value", "anatomic_site_source_value", "disease_status_source_value"],
                         "visit_occurrence": ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_datetime",
                                              "visit_end_date", "visit_end_datetime", "visit_type_concept_id", "provider_id", "care_site_id",
                                              "visit_source_value", "visit_source_concept_id", "admitting_source_concept_id", "admitting_source_value",
                                              "discharge_to_concept_id", "discharge_to_source_value", "preceding_visit_occurrence_id"]
                        }
        self.date_column_data = {
                "condition_occurrence": {"condition_start_datetime": "condition_start_date", "condition_end_datetime": "condition_end_date"},
                "death": {"death_datetime": "death_date"},
                "drug_exposure": {"drug_exposure_start_datetime": "drug_exposure_start_date", "drug_exposure_end_datetime": "drug_exposure_end_date"},
                "measurement": {"measurement_datetime": "measurement_date"},
                "observation": {"observation_datetime": "observation_date"},
                #"person": {"birth_datetime": ["year_of_birth", "month_of_birth", "day_of_birth"]},
                "procedure_occurrence": {"procedure_datetime": "procedure_date"},
                "specimen": {"specimen_datetime": "specimen_date"},
                "visit_occurrence": {"visit_start_datetime": "visit_start_date", "visit_end_datetime": "visit_end_date"}
                }
        self.datetime_columns = {
                "condition_occurrence": ["condition_start_datetime", "condition_end_datetime"],
                "death": ["death_datetime"],
                "drug_exposure": ["drug_exposure_start_datetime", "drug_exposure_end_datetime"],
                "measurement": ["measurement_datetime"],
                "observation": ["observation_datetime"],
                "person": ["birth_datetime"],
                "procedure_occurrence": ["procedure_datetime"],
                "specimen": ["specimen_datetime"],
                "visit_occurrence": ["visit_start_datetime", "visit_end_datetime"]
                }
        self.person_id_column = {
                "condition_occurrence": "person_id",
                "death": "person_id",
                "drug_exposure": "person_id",
                "measurement": "person_id",
                "observation": "person_id",
                "person": "person_id",
                "procedure_occurrence": "person_id",
                "specimen": "person_id",
                "visit_occurrence": "person_id"
                }
        self.auto_number_column = {
                "condition_occurrence": "condition_occurrence_id",
                "death": "death_id",
                "drug_exposure": "drug_exposure_id",
                "measurement": "measurement_id",
                "observation": "observation_id",
                "procedure_occurrence": "procedure_occurrence_id",
                "specimen": "specimen_id",
                "visit_occurrence": "visit_occurrence_id"
                }

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

    def get_omop_date_column_data(self, tablename):
        if tablename in self.date_column_data:
            return self.date_column_data[tablename]
        return {}

    def get_omop_datetime_fields(self, tablename):
        if tablename in self.datetime_columns:
            return self.datetime_columns[tablename]
        return None

    def get_omop_person_id_field(self, tablename):
        if tablename in self.person_id_column:
            return self.person_id_column[tablename]
        return None

    def get_omop_auto_number_field(self, tablename):
        if tablename in self.auto_number_column:
            return self.auto_number_column[tablename]
        return None
