import installKoral
import updateKoral
import manageKoral
from fabric.api import *

@roles('master','slaves')
def test():
    run("echo \"" + env.host + ": It works\"")

def install():
    execute(installKoral.installMaster)
    execute(installKoral.installSlave)

def update():
    execute(updateKoral.updateMaster)
    execute(updateKoral.updateSlave)

def updateConfig(configFile=-1):
    execute(updateKoral.updateConfig, configFile)

def start(remoteLogger=-1, remoteMeasurementCollector=-1):
    execute(manageKoral.startMaster, remoteLogger, remoteMeasurementCollector)
    execute(manageKoral.startSlave, remoteLogger, remoteMeasurementCollector)

def startMaster(remoteLogger=-1, remoteMeasurementCollector=-1, startStandAlone=-1):
    manageKoral.startMaster(remoteLogger, remoteMeasurementCollector, startStandAlone)

def stop():
    execute(manageKoral.stopSlave)
    execute(manageKoral.stopMaster)

def stopMaster():
    manageKoral.stopSlave()

def clear():
    execute(manageKoral.clearKoral)
