from coconnect.tools import mapping_pipeline_helpers
#from io import StringIO
#_csv_data = open('University of Nottingham_PANTHER ds_structural_mapping.csv').read()

structural_mapping = mapping_pipeline_helpers\
    .StructuralMapping\
    .to_json('University of Nottingham_PANTHER_AUTO_structural_mapping.json',
             save = 'panther_structural_mapping.json',
             strict = False,
             destination_tables = ['person','condition_occurrence','visit_occurrence']
    )



