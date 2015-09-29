import environment
from fabric.api import *

@roles('master')
def updateMaster():
    performCommonUpdateSteps()

@roles('slaves')
def updateSlave():
    performCommonUpdateSteps()

def performCommonUpdateSteps():
    updateCIDRE()

def updateCIDRE():
    with cd("cidre"):
        run("git pull")
        run("ant")
    run("cp cidre/build/cidre.jar cidre.jar");
    put("../cidreConfig.xml","cidreConfig.xml")