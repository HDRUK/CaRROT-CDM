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
            for destination_field,source in destination_table.items():
                source_field = source['source_field']
                source_table = source['source_table']

                table_name = f"{destination_table_name}_{destination_field}"#_{source_table}"
                #table_name = f"{destination_field}_{source_table}"
                dot.node(table_name,
                         label=destination_field,style='filled', fillcolor='yellow',shape='box')

                dot.edge(destination_table_name,table_name,dir='back')

                source_field_name =  f"{source_table}_{source_field}"
                
                dot.node(source_field_name,source_field)

                if 'operations' in source:
                    operations = source['operations']

                if 'term_mapping' in source and source['term_mapping'] is not None:
                    term_mapping = source['term_mapping']
                    #tmap = f'{table_name}_{source_table}_{source_field}'
                    #dot.node(tmap,label=json.dumps(term_mapping),style='filled', fillcolor='azure2',shape='box')
                    #dot.edge(tmap,source_field,dir='back')
                    #dot.edge(table_name,tmap,dir='back')
                    #for i,(k,v) in enumerate(term_mapping.items()):
                    #    label = f'"{k}":"{v}"'
                    dot.edge(table_name,source_field_name,dir='back',color='red')
                        
                else:                                                    
                    dot.edge(table_name,source_field_name,dir='back')

                
                dot.node(source_table,shape='box')
                dot.edge(source_field_name,source_table,dir='back')


    if render:
        dot.render('person.gv', view=True)  
        return
        
    return dot.pipe().decode('utf-8')

