import installCIDR
from fabric.api import *

def install():
    installCIDR.execute(installMaster)
    installCIDR.execute(installSlave)

