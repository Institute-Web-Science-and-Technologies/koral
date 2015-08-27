import environment
from fabric.api import *

@roles('master')
def installMaster():
    performCommonInstallationSteps()

@roles('slaves')
def installSlave():
    performCommonInstallationSteps()

def performCommonInstallationSteps():
    environment.initialiseVM()
    installOracleJDK8()
    installGit()
    installAnt()
    cloneCidre()
    resolveDependencies()

def installOracleJDK8():
    sudo("add-apt-repository -y ppa:webupd8team/java")
    sudo("apt-get update")
    sudo("printf \"oracle-java8-installer shared/accepted-oracle-license-v1-1 select true\" | /usr/bin/debconf-set-selections")
    sudo("apt-get -y install oracle-java8-installer")
    sudo("apt-get -y install oracle-java8-set-default")

def installGit():
    sudo("apt-get update")
    sudo("apt-get -y install git")

def installAnt():
    sudo("apt-get update")
    sudo("apt-get -y install ant")

def cloneCidre():
    run("git clone server.djanke-diss:/git/cidre.git")

def resolveDependencies():
    resolveCLI()

def resolveCLI():
    run("wget http://apache.mirror.digionline.de//commons/cli/binaries/commons-cli-1.3.1-bin.tar.gz")
    run("tar -xf commons-cli-1.3.1-bin.tar.gz")
    run("rm commons-cli-1.3.1-bin.tar.gz")
    run("mv commons-cli-1.3.1/commons-cli-1.3.1.jar cidre/lib/commons-cli-1.3.1.jar")
    run("rm -r commons-cli-1.3.1")
