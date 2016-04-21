import environment
from fabric.api import *

@roles('master')
def updateMaster():
    performCommonUpdateSteps()

@roles('slaves')
def updateSlave():
    performCommonUpdateSteps()

def performCommonUpdateSteps():
    updateKoral()

def updateKoral():
    with cd("koral"):
        run("git pull")
        run("ant")
    run("cp koral/build/koral.jar koral.jar");
    put("../koralConfig.xml","koralConfig.xml")
