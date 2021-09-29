import subprocess
from subprocess import Popen, PIPE

def run_bash_cmd(cmd):
    if isinstance(cmd,str):
        cmd = cmd.split(" ")
    elif not isinstance(cmd,list):
        raise Exception("run_bash_cmd must be passed a bash command as a str or a list")

    session = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = (x.decode("utf-8") for x in session.communicate())
    return stdout,stderr
