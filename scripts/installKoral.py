import environment
from fabric.api import *
from fabric.contrib.files import *

@roles('master')
def installMaster():
    performCommonInstallationSteps()
    installMetis()

@roles('slaves')
def installSlave():
    performCommonInstallationSteps()

def performCommonInstallationSteps():
    environment.initialiseVM()
    installOracleJDK8()
    installGit()
    installMaven()
    cloneKoral()
    buildKoral()
    with cd("koral"):
        put("koralConfig.xml","koralConfig.xml")

def installOracleJDK8():
    sudo("add-apt-repository -y ppa:webupd8team/java")
    sudo("apt-get update")
    sudo("printf \"oracle-java8-installer shared/accepted-oracle-license-v1-1 select true\" | /usr/bin/debconf-set-selections")
    sudo("apt-get -y install oracle-java8-installer")
    sudo("apt-get -y install oracle-java8-set-default")

def installGit():
    sudo("apt-get update")
    sudo("apt-get -y install git")

def installMaven():
    sudo("apt-get update")
    sudo("apt-get -y install maven")

def cloneKoral():
    run("git clone https://github.com/Institute-Web-Science-and-Technologies/koral.git")

def buildKoral():
    with cd("koral"):
        run("mvn package")

def installMetis():
    sudo("apt-get update")
    sudo("apt-get -y install metis")
