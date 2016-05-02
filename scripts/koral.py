import installKoral
import updateKoral
import manageKoral
from fabric.api import *

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

def stop():
    execute(manageKoral.stopSlave)
    execute(manageKoral.stopMaster)

def clear():
    execute(manageKoral.clearKoral)
