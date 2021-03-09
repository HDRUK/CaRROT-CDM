from subcommands.map import map

import click
import json
import logging

LOGFORMAT = "%(asctime)s | %(name)20.20s | %(levelname)6s | %(message)s"

@click.group()
@click.option("-l", "--loglevel", default="INFO")
def coconnect(loglevel):
    logging.basicConfig(level=getattr(logging, loglevel), format=LOGFORMAT)


coconnect.add_command(map, "map")


if __name__ == "__main__":
    coconnect()
