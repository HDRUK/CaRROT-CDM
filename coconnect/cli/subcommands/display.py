import click
import pandas
import json
import coconnect.tools as tools

@click.group(help='Commands for displaying various types of data and files.')
def display():
    pass

@click.command(help="Display a dataframe")
@click.argument('fname')
@click.option('--drop-na',is_flag=True)
@click.option('--markdown',is_flag=True)
@click.option('--head',type=int,default=None)
@click.option('--separator','--sep',type=str,default='\t')
def dataframe(fname,drop_na,markdown,head,separator):
    df = pandas.read_csv(fname,nrows=head,sep=separator)
    if drop_na:
        df = df.dropna(axis=1,how='all')
    if markdown:
        df = df.to_markdown()
    print (df)

@click.command(help="plot from a csv file")
@click.argument('fnames',nargs=-1)
@click.option('-y',required=True,multiple=True)
@click.option('-x',required=True)
def plot(fnames,x,y):
    import matplotlib.pyplot as plt
    fig,ax = plt.subplots(len(y),figsize=(14,7))

    dfs = {
        fname:pandas.read_csv(fname)
        for fname in fnames
    }
    
    for i,_y in enumerate(y):
        ax[i].set_ylabel(_y)
        for fname in fnames:
            dfs[fname].plot(x=x,y=_y,ax=ax[i],label=fname)
    plt.show()


@click.command(help="Display the OMOP mapping json as a DAG")
@click.argument("rules")
def dag(rules):
    data = tools.load_json(rules)
    tools.make_dag(data['cdm'],render=True) 


@click.command(help="Show the OMOP mapping json")
@click.argument("rules")
def print_json(rules):
    data = tools.load_json(rules)
    print (json.dumps(data,indent=6))


@click.command(help="Detect differences in either inputs or output csv files")
@click.option('--separator','--sep',type=str,default='\t')
@click.argument("file1")
@click.argument("file2")
def diff(file1,file2,separator):
    df1 = pandas.read_csv(file1,sep=separator)
    df2 = pandas.read_csv(file2,sep=separator)
    
    exact_match = df1.equals(df2)
    if exact_match:
        return

    df = pandas.concat([df1,df2]).drop_duplicates(keep=False)
    if len(df) > 0:
        print (" ======== Differing Rows ========== ")
        print (df)
        m = df1.merge(df2, on=df.columns[0], how='outer', suffixes=['', '_'], indicator=True)[['_merge']]
        m = m[~m['_merge'].str.contains('both')]
        file1 = file1.split('/')[-1]
        file2 = file2.split('/')[-1]
        
        m['_merge'] = m['_merge'].map({'left_only':file1,'right_only':file2})
        m = m.rename(columns={'_merge':'Only Contained Within'})
        m.index.name = 'Row Number'
        print (m.reset_index().to_dict(orient='records'))

    else:
        print (" ======= Rows are likely in a different order ====== ")
        for i in range(len(df1)):
            if not (df1.iloc[i] == df2.iloc[i]).any():
                print ('Row',i,'is in a different location')


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

                
display.add_command(dataframe,"dataframe")
display.add_command(dag,"dag")
display.add_command(print_json,"json")
display.add_command(plot,"plot")

display.add_command(diff,"diff")
display.add_command(flatten,"flatten")
