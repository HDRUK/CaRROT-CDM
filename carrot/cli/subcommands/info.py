import click
import os
import carrot

@click.group(help='Commands to find information about the package.')
def info():
    pass

@click.command(help="Get the data folder location")
def data_folder():
    _dir = os.path.dirname(os.path.abspath(carrot.__file__))
    data_dir = f"{_dir}{os.path.sep}data"
    print (data_dir)

@click.command(help="Get the root folder location of carrot tools")
def install_folder():
    _dir = os.path.dirname(os.path.abspath(carrot.__file__))
    print (_dir)

@click.command(help="Get the installed version of the package'")
def version():
    print (carrot.__version__)
    
info.add_command(install_folder,'install_folder')
info.add_command(data_folder,'data_folder')
info.add_command(version,'version')


