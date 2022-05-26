import click
import requests
import carrot
import hashlib
import os
from carrot.tools.logger import Logger

@click.command(help="Command to help pseudonymise data.")
@click.option("-s","--salt",help="salt hash",required=True,type=str)
@click.option("--person-id","-i","--id",help="name of the person_id",required=True,type=str)
@click.option("--output-folder","-o",help="path of the output folder",required=True,type=str)
@click.option("--chunksize",help="set the chunksize when loading data",type=int,default=None)
@click.argument("input",required=True)
def pseudonymise(input,output_folder,chunksize,salt,person_id):

    logger = Logger("pseudonymise")
    logger.info(f"Working on file {input}, pseudonymising column '{person_id}' with salt '{salt}'")
    
    #create the dir
    os.makedirs(output_folder,exist_ok=True)
    f_out = f"{output_folder}{os.path.sep}{os.path.basename(input)}"

    logger.info(f"Saving new file to {f_out}")
    
    #load data
    data = carrot.tools.load_csv(input,chunksize=chunksize)

    i = 0 
    while True:
        data[input][person_id] =  data[input][person_id].apply(
            lambda x: hashlib.sha256((x+salt).encode("UTF-8")).hexdigest()
        )
        logger.info(data[input][person_id])

        mode = 'w'
        header=True
        if i > 0 :
            mode = 'a'
            header=False
        
        data[input].to_csv(f_out,mode=mode,header=header,index=False)
        log.info(f"Finished {input} of size={len(data[input])} on iteration {i}")
        i+=1
        
        try:
            data.next()
        except StopIteration:
            break
    

    logger.info("Done!")
    return f_out
