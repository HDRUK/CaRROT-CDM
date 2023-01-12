class InputColumnData:

    def __init__(self):
        self.date_columns = {
            "gs_demog.csv": "yob",
            "gs_dolorisk.csv": "dolorisk_date",
            "gs_diabetes.csv": "diag_date",
            "gs_cases_icd10.csv": "case_date",
            "gs_cases_icd10_prev.csv": "appt",
            "gs_cases_icd9.csv": "case_date",
            "gs_cases_icd9_prev.csv": "appt",
            "gs_appt_events.csv": "appt",
            "SMR01_Demographics.csv": "dob",
            "SMR01_Condition.csv": "ADMISSION_DATE",
            "SMR01_Operations.csv": "DATE_OPERATION",
            "Serology.csv": "Date",
            "Vaccinations.csv": "date_of_vaccination",
            "GP_Records.csv": "date_of_visit",
            "Demographics.csv": "Age"
        }
        self.id_columns = {
            "gs_demog.csv": "id",
            "gs_dolorisk.csv": "id",
            "gs_diabetes.csv": "id",
            "gs_cases_icd10.csv": "id",
            "gs_cases_icd10_prev.csv": "id",
            "gs_cases_icd9.csv": "id",
            "gs_cases_icd9_prev.csv": "id",
            "gs_appt_events.csv": "id",
            "SMR01_Demographics.csv": "ENCRYPTED_UPI",
            "SMR01_Condition.csv": "ENCRYPTED_UPI",
            "SMR01_Operations.csv": "ENCRYPTED_UPI",
            "Serology.csv": "ID",
            "Vaccinations.csv": "ID",
            "GP_Records.csv": "ID",
            "Demographics.csv": "ID"
        }

    def is_data_column(self, colname, filename):
        if colname == self.date_columns[filename]:
            return False
        if colname == self.id_columns[filename]:
            return False
        return True

    def get_id_column(self, filename):
        if filename in self.id_columns:
            return self.id_columns[filename]
        return None

    def get_date_column(self, filename):
        if filename in self.date_columns:
            return self.date_columns[filename]
        return None
