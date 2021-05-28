import subprocess
from prefect import Task

class DBTTask(Task):
    def run(self, command):
        subprocess.call(command, shell=True)