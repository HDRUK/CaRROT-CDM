from .subcommands.info import info
from .subcommands.etl import etl
from .subcommands.display import display
from .subcommands.run import run
from .subcommands.airflow import airflow
from .subcommands.generate import generate

from .subcommands.get import get
from .subcommands.search import search
from .subcommands.pseudonymise import pseudonymise

from carrot.tools.logger import _Logger as Logger
import carrot as c

import click
import json


@click.group(invoke_without_command=True)
@click.option("--version","-v",is_flag=True)
@click.option("--log-level","-l",
              type=click.Choice(['0','1','2','3']),
              default='2',
              help="change the level for log messaging. 0 - ERROR, 1 - WARNING, 2 - INFO (default), 3 - DEBUG ")
@click.option("-cp", "--cprofile", is_flag=True, help='use cProfile to profile the tool')
@click.pass_context
def carrot(ctx,version,log_level,cprofile):
    if ctx.invoked_subcommand == None :
        if version:
            click.echo(c.__version__)
        else:
            click.echo(ctx.get_help()) 
        return
           

    c.params['debug_level'] = int(log_level)
    log = Logger("carrot")
    if cprofile:
        import cProfile
        log.info("Profiling...")
        pr = cProfile.Profile()
        pr.enable()

        def callback():
            pr.disable()
            log.info("Profiling completed")
            pr.dump_stats('carrot.prof')

        ctx.call_on_close(callback)

        
carrot.add_command(etl, "etl")
carrot.add_command(run, "run")
carrot.add_command(info, "info")
carrot.add_command(display, "display")
carrot.add_command(generate, "generate")
carrot.add_command(get, "get")
carrot.add_command(search, "search")
carrot.add_command(pseudonymise, "pseudonymise")
carrot.add_command(airflow,'airflow')



if __name__ == "__main__":
    carrot()
