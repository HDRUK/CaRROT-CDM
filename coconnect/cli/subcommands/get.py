import os
import click
import json
import coconnect
import pandas as pd
import numpy as np
import requests

class MissingToken(Exception):
    pass


@click.group(help='Commands to get data from the CCOM api.')
def get():
    pass


@click.command(help="get a json file")
@click.option("-i","--report-id",help="ScanReport ID on the website",required=True,type=int)
@click.option("-t","--token",help="specify the coconnect_token for accessing the CCOM website",type=str,default=None)
@click.option("-u","--url",help="url endpoint for the CCOM website to ping",
              type=str,
              default="https://ccom.azurewebsites.net")
def _json(report_id,token,url):

    token = os.environ.get("COCONNECT_TOKEN") or token
    if token == None:
        raise MissingToken("you must use the option --token or set the environment variable COCONNECT_TOKEN to be able to use this functionality. I.e  export COCONNECT_TOKEN=12345678 ")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        "Content-type": "application/json",
        "charset": "utf-8",
        "Authorization": f"Token {token}"
    }

    response = requests.get(
        f"{url}/api/json/?id={report_id}",
        headers=headers
    ).json()[0]

    print (json.dumps(response,indent=6))

    
get.add_command(_json,"json")
