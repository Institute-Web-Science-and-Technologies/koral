import environment
from fabric.api import *
from fabric.contrib.files import *

@roles('master')
def startMaster(remoteLogger=-1):
    additionalArgs = "";
    if (remoteLogger != -1):
        additionalArgs = " -r " + remoteLogger;
    run("nohup java -jar cidre.jar master" + additionalArgs + " >& /dev/null < /dev/null & echo $! > cidreMaster.pid &", pty=False)

@roles('master')
def stopMaster():
    with settings(warn_only=True):
        run("kill -15 $(cat cidreMaster.pid)")
        run("rm cidreMaster.pid")

@roles('slaves')
def startSlave(remoteLogger=-1):
    additionalArgs = "";
    if (remoteLogger != -1):
        additionalArgs = " -r " + remoteLogger;
    run("nohup java -jar cidre.jar slave" + additionalArgs + " >& /dev/null < /dev/null & echo $! > cidreSlave.pid &", pty=False)

@roles('slaves')
def stopSlave():
    with settings(warn_only=True):
        run("kill -15 $(cat cidreSlave.pid)")
        run("rm cidreSlave.pid")

@roles('master','slaves')
def clearCidre():
    dictDir = run("java -cp cidre.jar de.uni_koblenz.west.cidre.common.config.utils.ConfigCLI cidreConfig.xml dictionaryDir")
    if exists(dictDir, use_sudo=True):
        sudo("rm -r " + dictDir)
    statDir = run("java -cp cidre.jar de.uni_koblenz.west.cidre.common.config.utils.ConfigCLI cidreConfig.xml statisticsDir")
    if exists(statDir, use_sudo=True):
        sudo("rm -r " + statDir)
    tripleDir = run("java -cp cidre.jar de.uni_koblenz.west.cidre.common.config.utils.ConfigCLI cidreConfig.xml tripleStoreDir")
    if exists(tripleDir, use_sudo=True):
        sudo("rm -r " + tripleDir)
