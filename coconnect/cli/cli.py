from .subcommands.info import info
from .subcommands.display import display
from .subcommands.map import map
from .subcommands.generate import generate
from coconnect.tools.logger import Logger
import coconnect as cc

import click
import json


@click.group()
@click.option("--log-level","-l",
              type=click.Choice(['0','1','2','3']),
              default='2',
              help="change the level for log messaging. 0 - ERROR, 1 - WARNING, 2 - INFO (default), 3 - DEBUG ")
@click.option("-cp", "--cprofile", is_flag=True, help='use cProfile to profile the tool')
@click.pass_context
def coconnect(ctx,log_level,cprofile):
    cc.params['debug_level'] = int(log_level)
    log = Logger("coconnect")
    if cprofile:
        import cProfile
        log.info("Profiling...")
        pr = cProfile.Profile()
        pr.enable()

        def callback():
            pr.disable()
            log.info("Profiling completed")
            pr.dump_stats('coconnect.prof')

        ctx.call_on_close(callback)

        

coconnect.add_command(map, "map")
coconnect.add_command(info, "info")
coconnect.add_command(display, "display")
coconnect.add_command(generate, "generate")


if __name__ == "__main__":
    coconnect()
