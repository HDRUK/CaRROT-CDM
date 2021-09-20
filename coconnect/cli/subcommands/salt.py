import click
import requests
import coconnect
import hashlib
import os

@click.command(help="Command to help salt data.")
@click.option("-s","--salt",help="salt hash",required=True,type=str)
@click.option("--person-id","-i","--id",help="name of the person_id",required=True,type=str)
@click.option("--output-folder","-o",help="path of the output folder",required=True,type=str)
@click.argument("input",required=True)
def salt(input,output_folder,salt,person_id):
    data = coconnect.tools.load_csv(input,chunksize=10)

    i = 0 
    while True:
        data[input][person_id] =  data[input][person_id].apply(lambda x: hashlib.sha256(x.encode("UTF-8")).hexdigest())

        os.makedirs(output_folder,exist_ok=True)
        f_out = f"{output_folder}{os.path.sep}{os.path.basename(input)}"

        mode = 'w'
        header=True
        if i > 0 :
            mode = 'a'
            header=False
        
        data[input].to_csv(f_out,mode=mode,header=header,index=False)
        
        i+=1
        
        try:
            data.next()
        except StopIteration:
            break
    

