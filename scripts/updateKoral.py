import environment
from fabric.api import *

@roles('master')
def updateMaster(branch=-1):
    performCommonUpdateSteps(branch)

@roles('slaves')
def updateSlave(branch=-1):
    performCommonUpdateSteps(branch)

def performCommonUpdateSteps(branch=-1):
    updateKoral(branch)

def updateKoral(branch=-1):
    with cd("koral"):
        run("git pull")
        if (branch != -1):
          run("git checkout " + branch)
          run("git pull")
        run("mvn clean package")
    run("cp koral/target/koral.jar koral.jar")
    #put("koralConfig.xml","koralConfig.xml")

@roles('master','slaves')
def updateConfig(configFile=-1):
   if (configFile == -1):
      configFile = "koralConfig.xml"
   put(configFile,"koralConfig.xml")
