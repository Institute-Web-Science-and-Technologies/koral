import environment
from fabric.api import *

@roles('master')
def startMaster(remoteLogger=-1):
    additionalArgs = "";
    if (remoteLogger != -1):
        additionalArgs = " -r " + remoteLogger;
    run("nohup java -jar cidre.jar master" + additionalArgs + " >& /dev/null < /dev/null & echo $! > cidreMaster.pid &", pty=False)

@roles('master')
def stopMaster():
    run("kill $(cat cidreMaster.pid)")
    run("rm cidreMaster.pid")

@roles('slaves')
def startSlave(remoteLogger=-1):
    additionalArgs = "";
    if (remoteLogger != -1):
        additionalArgs = " -r " + remoteLogger;
    run("nohup java -jar cidre.jar slave" + additionalArgs + " >& /dev/null < /dev/null & echo $! > cidreSlave.pid &", pty=False)

@roles('slaves')
def stopSlave():
    run("kill $(cat cidreSlave.pid)")
    run("rm cidreSlave.pid")
