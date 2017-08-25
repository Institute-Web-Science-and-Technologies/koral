import environment
from fabric.api import *

@roles('master')
def updateMaster(branch=-1):
    performCommonUpdateSteps(branch)

@roles('slaves')
def updateSlave(branch=-1):
    performCommonUpdateSteps()

def performCommonUpdateSteps(branch=-1):
    updateKoral(branch)

def updateKoral(branch=-1):
    run("rm koral/lib/*.jar")
    run("sh koral/resolveDependencies.sh")
    with cd("koral"):
        run("git pull")
        if (branch != -1):
          run("git checkout " + branch)
          run("git pull")
        run("ant")
    run("cp koral/build/koral.jar koral.jar")
    put("../koralConfig.xml","koralConfig.xml")

@roles('master','slaves')
def updateConfig(configFile=-1):
   if (configFile == -1):
      configFile = "../koralConfig.xml"
   put(configFile,"koralConfig.xml")
