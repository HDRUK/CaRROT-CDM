from graphviz import Digraph
import json

def make_dag(data,render=False):
    _format = 'svg'
    if render == True:
        _format = 'pdf'
                
    dot = Digraph(strict=True,format=_format)
    dot.attr(rankdir='RL', size='8,5')

    
    for destination_table_name,destination_tables in data.items():
        dot.node(destination_table_name,shape='box')

        #destination_table = destination_tables[0]

        for destination_table in destination_tables:
            for table_name,source in destination_table.items():
                dot.node(table_name,style='filled', fillcolor='yellow',shape='box')
                dot.edge(destination_table_name,table_name,dir='back')
                
                source_field = source['source_field']
                source_table = source['source_table']

                if 'operations' in source:
                    operations = source['operations']

                if 'term_mapping' in source and source['term_mapping'] is not None:
                    term_mapping = source['term_mapping']
                    #tmap = f'{table_name}_{source_table}_{source_field}'
                    
                    #dot.node(tmap,label=json.dumps(term_mapping),style='filled', fillcolor='azure2',shape='box')
                    #dot.edge(tmap,source_field,dir='back')
                    #dot.edge(table_name,tmap,dir='back')
                    dot.edge(table_name,source_field,dir='back',label=json.dumps(term_mapping,indent=6))
                else:                                                    
                    dot.edge(table_name,source_field,dir='back')

                
                dot.node(source_table,shape='box')
                dot.edge(source_field,source_table,dir='back')


    if render:
        dot.render('person.gv', view=True)  
        return
        
    return dot.pipe().decode('utf-8')

