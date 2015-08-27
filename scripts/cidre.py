import installCIDRE
import updateCIDRE
from fabric.api import *

def install():
    execute(installCIDRE.installMaster)
    execute(installCIDRE.installSlave)

def update():
    execute(updateCIDRE.updateMaster)
    execute(updateCIDRE.updateSlave)
