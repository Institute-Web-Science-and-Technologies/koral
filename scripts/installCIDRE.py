import environment
from fabric.api import *
from fabric.contrib.files import *

@roles('master')
def installMaster():
    performCommonInstallationSteps()
    #installMetis()

@roles('slaves')
def installSlave():
    performCommonInstallationSteps()

def performCommonInstallationSteps():
    #environment.initialiseVM()
    #installOracleJDK8()
    #installGit()
    #installAnt()
    cloneCidre()
    resolveDependencies()
    buildCidre()
    put("../cidreConfig.xml","cidreConfig.xml")

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
    if exists("cidre", use_sudo=True):
        sudo("rm -r cidre")
    run("git clone server.djanke-diss:/git/cidre.git")

def resolveDependencies():
    resolveCLI()
    #installMaven()
    resolveJeromq()
    resolveJena()
    resolveMapDB()
    resolveFtpServer()
    resolveCommonsNet()

def resolveCLI():
    run("wget http://apache.mirror.digionline.de//commons/cli/binaries/commons-cli-1.3.1-bin.tar.gz")
    run("tar -xf commons-cli-1.3.1-bin.tar.gz")
    run("rm commons-cli-1.3.1-bin.tar.gz")
    run("mv commons-cli-1.3.1/commons-cli-1.3.1.jar cidre/lib/commons-cli-1.3.1.jar")
    run("rm -r commons-cli-1.3.1")

def resolveJeromq():
    run("git clone https://github.com/zeromq/jeromq.git")
    with cd("jeromq"):
        run("git checkout tags/v0.3.5")
        run("mvn package -DskipTests")
    run("cp jeromq/target/jeromq-0.3.5.jar cidre/lib/jeromq-0.3.5.jar")
    sudo("rm -r jeromq")

def installMaven():
    sudo("apt-get update")
    sudo("apt-get -y install maven")

def resolveJena():
    run("wget https://archive.apache.org/dist/jena/binaries/apache-jena-3.0.0.tar.gz")
    run("tar -xf apache-jena-3.0.0.tar.gz")
    run("rm apache-jena-3.0.0.tar.gz")
    run("mv apache-jena-3.0.0/lib/jena-arq-3.0.0.jar cidre/lib/jena-arq-3.0.0.jar")
    run("mv apache-jena-3.0.0/lib/jena-base-3.0.0.jar cidre/lib/jena-base-3.0.0.jar")
    run("mv apache-jena-3.0.0/lib/jena-core-3.0.0.jar cidre/lib/jena-core-3.0.0.jar")
    run("mv apache-jena-3.0.0/lib/jena-iri-3.0.0.jar cidre/lib/jena-iri-3.0.0.jar")
    run("mv apache-jena-3.0.0/lib/jena-shaded-guava-3.0.0.jar cidre/lib/jena-shaded-guava-3.0.0.jar")
    run("mv apache-jena-3.0.0/lib/xercesImpl-2.11.0.jar cidre/lib/xercesImpl-2.11.0.jar")
    run("mv apache-jena-3.0.0/lib/xml-apis-1.4.01.jar cidre/lib/xml-apis-1.4.01.jar")
    run("mv apache-jena-3.0.0/lib/slf4j-api-1.7.12.jar cidre/lib/slf4j-api-1.7.12.jar")
    run("rm -r apache-jena-3.0.0")
    # use logger that discards log messages
    run("wget http://www.slf4j.org/dist/slf4j-1.7.12.tar.gz")
    run("tar -xf slf4j-1.7.12.tar.gz")
    run("rm slf4j-1.7.12.tar.gz")
    run("mv slf4j-1.7.12/slf4j-nop-1.7.12.jar cidre/lib/slf4j-nop-1.7.12.jar")
    run("rm -r slf4j-1.7.12")

def resolveMapDB():
    run("git clone https://github.com/jankotek/mapdb.git")
    with cd("mapdb"):
        run("git checkout tags/mapdb-1.0.8")
        run("mvn package -DskipTests")
    run("cp mapdb/target/mapdb-1.0.8.jar cidre/lib/mapdb-1.0.8.jar")
    sudo("rm -r mapdb")

def resolveFtpServer():
    run("wget http://archive.apache.org/dist/mina/ftpserver/1.0.5/ftpserver-1.0.5.zip")
    run("unzip ftpserver-1.0.5.zip")
    run("rm ftpserver-1.0.5.zip")
    run("mv apache-ftpserver-1.0.5/common/lib/ftplet-api-1.0.5.jar cidre/lib/ftplet-api-1.0.5.jar")
    run("mv apache-ftpserver-1.0.5/common/lib/ftpserver-core-1.0.5.jar cidre/lib/ftpserver-core-1.0.5.jar")
    run("mv apache-ftpserver-1.0.5/common/lib/mina-core-2.0.0-RC1.jar cidre/lib/mina-core-2.0.0-RC1.jar")
    run("rm -r apache-ftpserver-1.0.5")

def resolveCommonsNet():
    run("wget http://mirror.softaculous.com/apache//commons/net/binaries/commons-net-3.4-bin.tar.gz")
    run("tar -xf commons-net-3.4-bin.tar.gz")
    run("rm commons-net-3.4-bin.tar.gz")
    run("mv commons-net-3.4/commons-net-3.4.jar cidre/lib/commons-net-3.4.jar")
    run("rm -r commons-net-3.4")

def buildCidre():
    with cd("cidre"):
        run("ant")
    run("cp cidre/build/cidre.jar cidre.jar");

def installMetis():
    sudo("apt-get update")
    sudo("apt-get -y install metis")
