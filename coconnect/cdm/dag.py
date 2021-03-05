from graphviz import Digraph
import json

def make_dag(data):
    dot = Digraph(strict=True,format='svg')
    dot.attr(rankdir='LR', size='8,5')
    dot.node('person',shape='box')

    for table_name,source in data.items():
        dot.node(table_name,style='filled', fillcolor='yellow',shape='box')
        dot.edge('person',table_name,dir='back')

        source_field = source['source_field']
        source_table = source['source_table']

        
        dot.edge(table_name,source_field,dir='back')
        
        dot.node(source_table,shape='box')
        dot.edge(source_field,source_table,dir='back')
    

    return dot.pipe().decode('utf-8')

