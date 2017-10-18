import environment
from fabric.api import *
from fabric.contrib.files import *

@roles('master')
def startMaster(remoteLogger=-1, remoteMeasurementCollector=-1, startStandAlone=-1):
    additionalArgs = "";
    if (remoteLogger != -1):
        additionalArgs += " -r " + remoteLogger;
    if (remoteMeasurementCollector != -1):
        additionalArgs += " -m " + remoteMeasurementCollector;
    if (startStandAlone != -1):
        additionalArgs += " -o ";
    run("nohup java -jar koral/target/koral.jar master" + additionalArgs + " >& master.out < /dev/null & echo $! > koralMaster.pid &", pty=False)

@roles('master')
def stopMaster():
    with settings(warn_only=True):
        run("kill -15 $(cat koralMaster.pid)")
        run("rm koralMaster.pid")

@roles('slaves')
def startSlave(remoteLogger=-1, remoteMeasurementCollector=-1):
    additionalArgs = "";
    if (remoteLogger != -1):
        additionalArgs += " -r " + remoteLogger;
    if (remoteMeasurementCollector != -1):
        additionalArgs += " -m " + remoteMeasurementCollector;
    run("nohup java -jar koral/target/koral.jar slave" + additionalArgs + " >& slave.out < /dev/null & echo $! > koralSlave.pid &", pty=False)

@roles('slaves')
def stopSlave():
    with settings(warn_only=True):
        run("kill -15 $(cat koralSlave.pid)")
        run("rm koralSlave.pid")

@roles('master','slaves')
def clearKoral():
    with cd("koral"):
        dictDir = run("java -cp target/koral.jar de.uni_koblenz.west.koral.common.config.utils.ConfigCLI koralConfig.xml dictionaryDir")
        if exists(dictDir, use_sudo=True):
            run("rm -r " + dictDir)
        statDir = run("java -cp target/koral.jar de.uni_koblenz.west.koral.common.config.utils.ConfigCLI koralConfig.xml statisticsDir")
        if exists(statDir, use_sudo=True):
            run("rm -r " + statDir)
        tripleDir = run("java -cp target/koral.jar de.uni_koblenz.west.koral.common.config.utils.ConfigCLI koralConfig.xml tripleStoreDir")
        if exists(tripleDir, use_sudo=True):
            run("rm -r " + tripleDir)
