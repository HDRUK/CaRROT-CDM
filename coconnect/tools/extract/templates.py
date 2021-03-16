from jinja2 import Template

cls = Template(r'''
from coconnect.cdm import define_person, define_condition_occurrence, define_measurement, load_csv
from coconnect.cdm import CommonDataModel
import json

class {{ name }}(CommonDataModel):

    {% for object in objects -%}
    {{ object }}
    {%- endfor %}


if __name__ == '__main__':
    {{ name }}()

''')

obj = Template(r'''
    @define_{{ object_name }}
    def {{ function_name }}(self):
        {% for rule in map_rules -%}
        {{ rule }}
        {% endfor %}

''')

