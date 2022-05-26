import os
import click
import pandas
import json
import carrot.tools as tools
import carrot
import pandas as pd
import numpy as np
import datetime

@click.group(help='Commands for displaying various types of data and files.')
def display():
    pass

@click.group(help='Commands for displaying various help for the CDM')
def cdm():
    pass


@click.command(help="Display a table for the CDM")
@click.argument('names',required=True,nargs=-1)
@click.option('--markdown',is_flag=True)
@click.option('--latex',is_flag=True)
def table(names,markdown,latex):
    for name in names:
        obj = carrot.cdm.get_cdm_class(name)()
        data = []
        for field in obj.fields:
            pk = obj[field].pk
            if pk == True:
                pk = '✔'
            else:
                pk = ' '
            
            data.append([field,pk,obj[field].dtype])

        df = pd.DataFrame(data, columns=['name','pk','dtype'])#.set_index('name')
        df['table'] = name
        df.set_index('table',inplace=True)
        df.set_index('name',inplace=True)
        
        if markdown:
            df = df.to_markdown()
        elif latex:
            df = df.to_latex()
            
        print (df)

@click.command(help="Display a dataframe")
@click.argument('fname')
@click.option('--drop-col',multiple=True)
@click.option('--drop-na',is_flag=True)
@click.option('--markdown',is_flag=True)
@click.option('--latex',is_flag=True)
@click.option('--head',type=int,default=None)
@click.option('--sample',type=int,default=None)
@click.option('--separator','--sep',type=str,default=None)
def dataframe(fname,drop_na,drop_col,markdown,head,sample,separator,latex):

    #if separator not specified, get it from the file extension 
    if separator == None:
        separator = tools.get_separator_from_filename(fname)


    if sample and head:
        head = None
    df = pandas.read_csv(fname,nrows=head,sep=separator)
    if sample:
        df = df.sample(sample)
    df.set_index(df.columns[0],inplace=True)
    if drop_na:
        df = df.dropna(axis=1,how='all')

    drop_col = list(drop_col)
    df = df.drop(drop_col,axis=1)
        
    if markdown:
        df = df.to_markdown()
    elif latex:
        df = df.to_latex()

    print (df)

@click.command(help="plot from a csv file")
@click.argument('fnames',nargs=-1)
@click.option('-y',required=True,multiple=True)
@click.option('-x',required=True)
@click.option('--nbins',default=None,help='bin and take the average',type=int)
@click.option('--save-plot',default=None,help='choose the name of file to save the plot to')
def plot(fnames,x,y,save_plot,nbins):
    import matplotlib.pyplot as plt
    fig,ax = plt.subplots(len(y),figsize=(14,7))

    dfs = {
        fname:pandas.read_csv(fname)
        for fname in fnames
    }
    
    for i,_y in enumerate(y):
        ax[i].set_ylabel(_y)
        for fname in fnames:
            if not nbins:
                dfs[fname].plot(x=x,y=_y,ax=ax[i],label=fname)
            else:
                dx = dfs[fname][x]
                dy = dfs[fname][_y]
                sums, edges = np.histogram(dx, bins=nbins, weights=dy)
                print (sums)
                counts, _ = np.histogram(dx, bins=nbins)
                counts = sums / counts
                ax[i].hist(edges[1:],weights=counts,histtype='step')
                pass
    plt.show()
    if save_plot:
        fig.savefig(save_plot)


@click.command(help="Display the OMOP mapping json as a DAG")
@click.option('--orientation',default='RL',type=click.Choice(['RL','LR','TB','BT']))
@click.option('--show-concepts',is_flag=True,help="also show the concepts")
@click.option('--output-file',default='dag.gv',help="output file name")
@click.option('--format',default='pdf',help="file extension")
@click.argument("rules")
def dag(rules,orientation,show_concepts,output_file,format):
    data = tools.load_json(rules)
    if 'cdm' in data:
        data = data['cdm']
    tools.make_dag(data,output_file=output_file,format=format,orientation=orientation,render=True,show_concepts=show_concepts)

@click.command(help="Display the report json as a DAG")
@click.option('--orientation',default='RL',type=click.Choice(['RL','LR','TB','BT']))
@click.argument("f")
def report_dag(f,orientation):
    data = tools.load_json(f)
    tools.make_report_dag(data,orientation=orientation,render=True)


@click.command(help="Show the OMOP mapping json")
@click.argument("rules")
@click.option('--list-tables',is_flag=True)
@click.option('--list-fields',is_flag=True)
@click.option('operations','--add-operation',default=None,type=str)
def print_json(rules,list_fields,list_tables,operations):
    data = tools.load_json(rules)
    if list_fields or list_tables:
        data = tools.get_mapped_fields_from_rules(data)
        if list_tables:
            data = list(data.keys())

    if operations:
        operations = tools.load_json(operations)
        for field_name,operation in operations.items():
            print (field_name,operation)
        
    print (json.dumps(data,indent=6))


@click.command(help="Detect differences in either inputs or output csv files")
@click.option('--separator','--sep',type=str,default=None)
@click.option('--max-rows','-n',type=int,default=None)
@click.argument("file1")
@click.argument("file2")
def diff(file1,file2,separator,max_rows):
    tools.diff_csv(file1,file2,separator=separator,nrows=max_rows)

@click.command(help="display a delta of two rules files")
@click.argument("rules",nargs=2)
def delta(rules):
    r1,r2 = rules
    delta = carrot.tools.load_json_delta(r2,r1)
    dt = str(datetime.datetime.now())
    delta['metadata']['date_created'] = dt
    click.echo(json.dumps(delta,indent=6))
    
@click.command(help="flattern a rules json file")
@click.argument("rules")
def flatten(rules):
    data = tools.load_json(rules)
    objects = data['cdm']
    for destination_table,rule_set in objects.items():
        if len(rule_set) < 2: continue
        #print (rule_set)
        df = pd.DataFrame.from_records(rule_set).T

        #for name in df.index:
        #    print (name,len(df.loc[name]))

        df = df.loc['condition_concept_id'].apply(pd.Series)


        def merge(s):
            if s == 'term_mapping':
                print ('hiya')
                return {k:v for a in s for k,v in a.items()}
        
        print (df.groupby('source_field').agg(merge))
            
        #print (df.iloc[1])
        #print (df.iloc[1][1])
        #print (df)
        #print (df.iloc[0])
        #print (df.iloc[0].name)
        #print (df.iloc[0].apply(pd.Series))
        #print (df.iloc[1].apply(pd.Series)['term_mapping'].apply(pd.Series))
        
        exit(0)


cdm.add_command(table,"table")
display.add_command(cdm,"cdm")

display.add_command(dataframe,"dataframe")
display.add_command(plot,"plot")
display.add_command(diff,"diff")

@click.group(help='Commands for displaying json rules in various ways.')
def rules():
    pass


rules.add_command(delta,"delta")
rules.add_command(dag,"dag")
rules.add_command(print_json,"json")
rules.add_command(flatten,"flatten")
display.add_command(rules,"rules")


@click.group(help='Commands for displaying json report in various ways.')
def report():
    pass


report.add_command(print_json,"json")
report.add_command(report_dag,"dag")


display.add_command(report,"report")
