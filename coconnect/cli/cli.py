from .subcommands.map import map
from .subcommands.find import find
from .subcommands.generate import generate

import click
import json
import logging

LOGFORMAT = "%(asctime)s | %(name)20.20s | %(levelname)6s | %(message)s"

@click.group()
@click.option("-l", "--loglevel", default="INFO")
def coconnect(loglevel):
    logging.basicConfig(level=getattr(logging, loglevel), format=LOGFORMAT)


@click.group()
def info():
    pass

@click.command(help="Get the data folder location")
def data_folder():
    import os
    import coconnect
    _dir = os.path.dirname(os.path.abspath(coconnect.__file__))
    data_dir = f"{_dir}/data"
    print (data_dir)
    
info.add_command(data_folder,'data_folder')

@click.group()
def display():
    pass

@click.command(help="Display a dataframe")
@click.argument('fname')
@click.option('--drop-na',is_flag=True)
def dataframe(fname,drop_na):
    import pandas 
    df = pandas.read_csv(fname)
    if drop_na:
        df = df.dropna(axis=1,how='all')
    print (df)
    
display.add_command(dataframe,"dataframe")


coconnect.add_command(info, "info")
coconnect.add_command(display, "display")
coconnect.add_command(map, "map")
coconnect.add_command(find, "find")
coconnect.add_command(generate, "generate")


if __name__ == "__main__":
    coconnect()
