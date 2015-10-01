import installCIDRE
import updateCIDRE
import manageCIDRE
from fabric.api import *

def install():
    execute(installCIDRE.installMaster)
    execute(installCIDRE.installSlave)

def update():
    execute(updateCIDRE.updateMaster)
    execute(updateCIDRE.updateSlave)

def start(remoteLogger=-1):
    execute(manageCIDRE.startMaster, remoteLogger)
    execute(manageCIDRE.startSlave, remoteLogger)

def stop():
    execute(manageCIDRE.stopSlave)
    execute(manageCIDRE.stopMaster)
