import subprocess
from subprocess import Popen, PIPE
from .logger import Logger


class BashHelpers:

    def __init__(self):
        self.__logger = Logger("run_bash_cmd")

    def run_bash_cmd(self,cmd):
        if isinstance(cmd,str):
            cmd = cmd.split(" ")
        elif not isinstance(cmd,list):
            raise Exception("run_bash_cmd must be passed a bash command as a str or a list")
        self.__logger.notice(cmd)
        session = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = (x.decode("utf-8") for x in session.communicate())
        return stdout,stderr
