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


coconnect.add_command(map, "map")
coconnect.add_command(find, "find")
coconnect.add_command(generate, "generate")


if __name__ == "__main__":
    coconnect()
