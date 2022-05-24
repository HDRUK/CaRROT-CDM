from jinja2 import Template

cls = Template(r'''from carrot.cdm import define_person, define_condition_occurrence, define_visit_occurrence, define_measurement, define_observation, define_drug_exposure
from carrot.cdm import CommonDataModel
import json

class {{ name }}(CommonDataModel):
    {{ init }}
    {% for object in objects -%}
    {{ object }}
    {%- endfor %}

if __name__ == '__main__':
    {{ name }}()

''')

init = Template(r'''
    def __init__(self,**kwargs):
        """ 
        initialise the inputs and setup indexing 
        """
        super().__init__(**kwargs)
        {% if person_ids %}
        #set primary key indexing so tables can be linked
        self.set_indexing({{ person_ids }})
        {% endif %}
''')

'''
{% for ds,pk in person_ids.items() -%}
        self.inputs["{{ ds }}"].index = self.inputs["{{ ds }}"]["{{ pk }}"].rename('index')
        {% endfor %} 
'''

rule = Template(r'''self.{{ destination_field }}.series = self.inputs["{{ source_table }}"]["{{ source_field }}"]''')
operation = Template(r'''self.{{ destination_field }}.series = self.tools.{{ operation }}(self.{{ destination_field }}.series)''')

obj = Template(r'''
    @define_{{ object_name }}
    def {{ function_name }}(self):
        """
        Create CDM object for {{ object_name }}
        """
        {% for rule in map_rules -%}
        {{ rule }}
        {% endfor %}
''')

