import click
import pandas
import coconnect.tools as tools

@click.group(help='Commands for displaying various types of data and files.')
def display():
    pass

@click.command(help="Display a dataframe")
@click.argument('fname')
@click.option('--drop-na',is_flag=True)
@click.option('--markdown',is_flag=True)
def dataframe(fname,drop_na,markdown):
    df = pandas.read_csv(fname)
    if drop_na:
        df = df.dropna(axis=1,how='all')
    if markdown:
        df = df.to_markdown()
    print (df)

@click.command(help="plot from a csv file")
@click.argument('fname')
@click.option('-y',required=True,multiple=True)
@click.option('-x',required=True)
def plot(fname,x,y):
    
    import matplotlib.pyplot as plt
    df = pandas.read_csv(fname)
    fig,ax = plt.subplots(len(y),figsize=(14,7))
    for i,_y in enumerate(y):
        ax[i].set_ylabel(_y)
        df.plot(x=x,y=_y,ax=ax[i])
    plt.show()


@click.command(help="Display the OMOP mapping json as a DAG")
@click.argument("rules")
def dag(rules):
    data = tools.load_json(rules)
    tools.make_dag(data['cdm'],render=True) 


@click.command(help="Show the OMOP mapping json")
@click.argument("rules")
def json(rules):
    data = tools.load_json(rules)
    print (json.dumps(data,indent=6))

    
display.add_command(dataframe,"dataframe")
display.add_command(dag,"dag")
display.add_command(json,"json")
display.add_command(plot,"plot")

