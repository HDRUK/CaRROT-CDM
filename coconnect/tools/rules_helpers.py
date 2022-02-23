
class TableNotFoundError(Exception):
    pass
class FieldNotFoundError(Exception):
    pass

def get_person_ids(rules):
    return {
        subtable['person_id']['source_table']:subtable['person_id']['source_field'] 
        for table in rules['cdm'].values() 
        for subtable in table.values()
    }


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
    table = inputs[name]
    table.name = name
    return inputs[name]


def load_from_file(this):
    df = this.inputs[this.fname].dropna(axis=1)
    for colname in df.columns:
        this[colname].series = df[colname]


def apply_rules(this,rules,inputs=None):
    this.logger.info("Called apply_rules")

    if inputs is None:
        inputs = this.inputs

    this._meta['source_files'] = {}
    for destination_field,rule in rules.items():
        source_table_name = rule['source_table']
        source_field_name = rule['source_field']
        operations = None
        if 'operations' in rule:
            operations = rule['operations']
        term_mapping = None
        if 'term_mapping' in rule:
            term_mapping = rule['term_mapping']

        source_table = get_source_table(inputs,source_table_name)
        source_field = get_source_field(source_table,source_field_name)
        series = source_field.copy()

        if operations is not None:
            for operation in operations:
                function = this.tools[operation]
                series = function(series)
                
        if term_mapping is not None:
            if isinstance(term_mapping,dict):
                # value level mapping
                # - term_mapping is a dictionary between values and concepts
                # - map values in the input data, based on this map

                #need to make the value a string for mapping
                #pandas has a weird behaviour that when the value is an Int
                #the resulting series is a float64
                term_mapping = {k:str(v) for k,v in term_mapping.items()}
                series = series.map(term_mapping)
            else:
                # field level mapping.
                # - term_mapping is the concept_id
                # - set all values in this column to it
                series.values[:] = term_mapping

        this[destination_field].series = series
        this._meta['source_files'][destination_field] = {'table':source_table_name,'field':source_field_name}
        this.logger.info(f"Mapped {destination_field}")
