from graphviz import Digraph
import json

colorscheme = 'gnbu9'

def make_dag_from_objects(object_dict):
    _format = 'pdf'
    dot = Digraph(strict=True,format=_format)
    dot.attr(size='5,5')

    for destination_table,object_list in object_dict.items():
        dot.node(destination_table,shape='box')
        for obj in object_list:
            dot.node(obj,style='filled', fillcolor='yellow',shape='box')
            dot.edge(destination_table,obj,dir='forward')
                        
    dot.render('dag.gv', view=True)  
    return

def make_dag(data,output_file='dag.gv',format='pdf',render=False,orientation='RL',show_concepts=False):
    _format = format
    if render == False:
        _format = 'svg'

    dot = Digraph(strict=True,format=_format)
    dot.attr(rankdir=orientation)#, size='20,8')
            
    # #dot.attr(rankdir='RL', size='8,16',compound='true')
    
    with dot.subgraph(name='cluster_0') as dest, \
         dot.subgraph(name='cluster_1') as inp,\
         dot.subgraph(name='cluster_01') as c:
        
        #dest.attr(style='dotted',penwidth='4', label='CDM')
        #inp.attr(style='filled', fillcolor='lightgrey', penwidth='0', label='Input')
        
        dest.attr(style='filled', fillcolor='2', colorscheme='blues9', penwidth='0', label='Common Data Model')
        inp.attr(style='filled', fillcolor='2', colorscheme='greens9', penwidth='0', label='Source')
        if show_concepts:
            c.attr(style='filled', fillcolor='1', colorscheme='greens9', penwidth='0', label='Concepts')
        
        for destination_table_name,destination_tables in data.items():
            dest.node(destination_table_name,
                      shape='folder',style='filled',
                      fontcolor='white',colorscheme=colorscheme,
                      fillcolor='9')

            for ref_name,destination_table in destination_tables.items():
                for destination_field,source in destination_table.items():
                    source_field = source['source_field']
                    source_table = source['source_table']
                    
                    table_name = f"{destination_table_name}_{destination_field}"
                    dest.node(table_name,
                             label=destination_field,style='filled,rounded', colorscheme=colorscheme,
                             fillcolor='7',shape='box',fontcolor='white')

                    dest.edge(destination_table_name,table_name,arrowhead='none')

                    source_field_name =  f"{source_table}_{source_field}"
                    inp.node(source_field_name,source_field,
                             colorscheme=colorscheme,
                             style='filled,rounded',fillcolor='5',shape='box')
                    
                    if 'operations' in source:
                        operations = source['operations']

                    if 'term_mapping' in source and source['term_mapping'] is not None and show_concepts:
                        term_mapping = source['term_mapping']
                        if isinstance(term_mapping,dict):
                            concepts = list(term_mapping.values())
                        else:
                            concepts = [term_mapping]

                        for concept in concepts:
                            ref_name = ref_name.rsplit(" ",1)[0]
                            c.node(str(concept),label=ref_name,style='filled', colorscheme=colorscheme,
                                   fillcolor='black',shape='box',fontcolor='white')
                            dot.edge(table_name,str(concept),dir='back',color='red',penwidth='2')
                            dot.edge(str(concept),source_field_name,dir='back',color='red',penwidth='2')
                    else:                                                    
                        dot.edge(table_name,source_field_name,dir='back',penwidth='2')
                    
                    inp.node(source_table,shape='tab',fillcolor='4',colorscheme=colorscheme,style='filled')
                    inp.edge(source_field_name,source_table,arrowhead='none')#,dir='back',arrowhead='none')


    #dot.subgraph(destinations)
    #dot.subgraph(sources)

    #destinations = dot.subgraph(name='cdm')
    #destinations = Digraph('cdm')
    #destinations.attr(style='dotted',rank='same',label='process #2')
    #dot.subgraph(destinations)
    #sources = Digraph('sources')
    #sources.attr(style='dotted',rank='same',label='process #1')

    
                
    if render:
        dot.render(output_file, view=True)  
        return
    #    
    return dot.pipe().decode('utf-8')

def make_report_dag(data,name='dag',render=False,orientation='RL'):
    _format = 'svg'
    if render == True:
        _format = 'pdf'

    dot = Digraph(strict=True,format=_format)
    dot.attr(rankdir=orientation)#, size='20,8')

    colorscheme = 'bupu9'
    colorscheme = 'rdbu10'
    
    for table in data:

        dataset=table['meta']['dataset']
        dot.node(dataset,
             style='filled', colorscheme=colorscheme,
             fillcolor='10',shape='box',fontcolor='white')
        
        table_name = table['table']

        dot.node(table_name,
                  style='filled', colorscheme=colorscheme,
                  fillcolor='7',shape='box')#,fontcolor='white')
        n = table['meta']['nscanned']
        dot.edge(table_name,dataset,arrowhead='none',label=f"N={n}")
        
        for field in table['fields']:
            field_name = field['field']

            ref_name = table_name+field_name
            dot.node(ref_name,label=field_name,
                  style='filled,rounded', colorscheme=colorscheme,
                  fillcolor='6',shape='box')#,fontcolor='white')
            dot.edge(ref_name,table_name,arrowhead='none')
    
            for value in field['values']:
                frequency = int(round(value['frequency']*100.,0))
                if frequency < 5:
                    continue
                frequency = f"{frequency} %"

                value_name = value['value']
                ref_name_v = table_name+field_name+value_name
                dot.node(ref_name_v,label=value_name,
                         style='filled,rounded', colorscheme=colorscheme,
                         fillcolor='5',shape='box')#,fontcolor='white')
                dot.edge(ref_name_v,ref_name,label=frequency,arrowhead='none')

                if not 'concepts' in value:
                    continue

                for concept in value['concepts']:
                    concept_id = concept['concept_id']
                    concept_name = concept['concept_name']
                    domain_id = concept['domain_id']
                    label = concept_name+"\n"+str(concept_id)+' | '+domain_id
                    
                    ref_name_v2 = str(concept_id)+ ref_name_v
                    dot.node(ref_name_v2,label=label,
                             style='filled,rounded', colorscheme=colorscheme,
                             fillcolor='3',shape='box',fontcolor='white')
                    dot.edge(ref_name_v2,ref_name_v,arrowhead='none')

                    #dot.node(domain_id,
                    #         style='filled,rounded', colorscheme=colorscheme,
                    #         fillcolor='2',shape='box',fontcolor='black')
                    #dot.edge(domain_id,ref_name_v2,arrowhead='none')

    
    if render:
        dot.render(f'{name}.gv', view=True)  
        return
