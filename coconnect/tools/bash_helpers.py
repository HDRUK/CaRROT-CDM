import subprocess
from subprocess import Popen, PIPE
from .logger import Logger


class BashHelpers(Logger):

    def __init__(self,dry_run=False):
        self.dry_run = dry_run

    def run_bash_cmd(self,cmd):
        if isinstance(cmd,str):
            cmd = cmd.split(" ")
        elif not isinstance(cmd,list):
            raise Exception("run_bash_cmd must be passed a bash command as a str or a list")
        self.logger.notice(" ".join(cmd))
        if self.dry_run:
            return None,None
        session = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = (x.decode("utf-8") for x in session.communicate())

        if 'ERROR' in stderr:
            self.logger.critical(stderr)
            raise Exception("failled executing bash command")
        return stdout,stderr
