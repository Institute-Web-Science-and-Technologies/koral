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
    installAnt()
    cloneKoral()
    resolveDependencies()
    buildKoral()
    put("../koralConfig.xml","koralConfig.xml")

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

def cloneKoral():
    if exists("koral", use_sudo=True):
        sudo("rm -r koral")
    #run("git clone server.djanke-diss:/git/koral.git")
    run("git clone https://github.com/Institute-Web-Science-and-Technologies/koral.git")

def resolveDependencies():
    resolveCLI()
    installMaven()
    resolveJeromq()
    resolveJena()
    resolveMapDB()
    resolveFtpServer()
    resolveCommonsNet()
    resolveRocksDB()
    resolveSQLite()

def resolveCLI():
    run("wget http://artfiles.org/apache.org//commons/cli/binaries/commons-cli-1.4-bin.tar.gz")
    run("tar -xf commons-cli-1.4-bin.tar.gz")
    run("rm commons-cli-1.4-bin.tar.gz")
    run("mv commons-cli-1.4/commons-cli-1.4.jar koral/lib/commons-cli-1.4.jar")
    run("rm -r commons-cli-1.4")

def resolveJeromq():
    run("wget http://central.maven.org/maven2/org/zeromq/jeromq/0.4.0/jeromq-0.4.0.jar")
    run("mv jeromq-0.4.0.jar koral/lib/jeromq-0.4.0.jar")

def installMaven():
    sudo("apt-get update")
    sudo("apt-get -y install maven")

def resolveJena():
    run("wget https://archive.apache.org/dist/jena/binaries/apache-jena-3.3.0.tar.gz")
    run("tar -xf apache-jena-3.3.0.tar.gz")
    run("rm apache-jena-3.3.0.tar.gz")
    run("mv apache-jena-3.3.0/lib/jena-arq-3.3.0.jar koral/lib/jena-arq-3.3.0.jar")
    run("mv apache-jena-3.3.0/lib/jena-base-3.3.0.jar koral/lib/jena-base-3.3.0.jar")
    run("mv apache-jena-3.3.0/lib/jena-core-3.3.0.jar koral/lib/jena-core-3.3.0.jar")
    run("mv apache-jena-3.3.0/lib/jena-iri-3.3.0.jar koral/lib/jena-iri-3.3.0.jar")
    run("mv apache-jena-3.3.0/lib/jena-shaded-guava-3.3.0.jar koral/lib/jena-shaded-guava-3.3.0.jar")
    run("mv apache-jena-3.3.0/lib/xercesImpl-2.11.0.jar koral/lib/xercesImpl-2.11.0.jar")
    run("mv apache-jena-3.3.0/lib/xml-apis-1.4.01.jar koral/lib/xml-apis-1.4.01.jar")
    run("mv apache-jena-3.3.0/lib/slf4j-api-1.7.21.jar koral/lib/slf4j-api-1.7.21.jar")
    run("rm -r apache-jena-3.3.0")
    # use logger that discards log messages
    run("wget http://www.slf4j.org/dist/slf4j-1.7.25.tar.gz")
    run("tar -xf slf4j-1.7.25.tar.gz")
    run("rm slf4j-1.7.25.tar.gz")
    run("mv slf4j-1.7.25/slf4j-nop-1.7.25.jar koral/lib/slf4j-nop-1.7.25.jar")
    run("rm -r slf4j-1.7.25")

def resolveMapDB():
    run("wget http://central.maven.org/maven2/org/mapdb/mapdb/3.0.4/mapdb-3.0.4.jar")
    run("mv mapdb-3.0.4.jar koral/lib/mapdb-3.0.4.jar")

def resolveFtpServer():
    run("wget http://archive.apache.org/dist/mina/ftpserver/1.1.1/ftpserver-1.1.1.zip")
    run("unzip ftpserver-1.1.1.zip")
    run("rm ftpserver-1.1.1.zip")
    run("mv apache-ftpserver-1.1.1/common/lib/ftplet-api-1.1.1.jar koral/lib/ftplet-api-1.1.1.jar")
    run("mv apache-ftpserver-1.1.1/common/lib/ftpserver-core-1.1.1.jar koral/lib/ftpserver-core-1.1.1.jar")
    run("mv apache-ftpserver-1.1.1/common/lib/mina-core-2.0.16.jar koral/lib/mina-core-2.0.16.jar")
    run("rm -r apache-ftpserver-1.1.1")

def resolveCommonsNet():
    run("wget https://archive.apache.org/dist/commons/net/binaries/commons-net-3.6-bin.tar.gz")
    run("tar -xf commons-net-3.6-bin.tar.gz")
    run("rm commons-net-3.6-bin.tar.gz")
    run("mv commons-net-3.6/commons-net-3.6.jar koral/lib/commons-net-3.6.jar")
    run("rm -r commons-net-3.6")

def resolveRocksDB():
    run("wget http://central.maven.org/maven2/org/rocksdb/rocksdbjni/5.4.5/rocksdbjni-5.4.5.jar")
    run("mv rocksdbjni-5.4.5.jar koral/lib/rocksdbjni-5.4.5.jar")
    run("wget http://central.maven.org/maven2/org/osgi/org.osgi.core/6.0.0/org.osgi.core-6.0.0.jar")
    run("mv org.osgi.core-6.0.0.jar koral/lib/org.osgi.core-6.0.0.jar")
    run("wget http://central.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.4/snappy-java-1.1.4.jar")
    run("mv snappy-java-1.1.4.jar koral/lib/snappy-java-1.1.4.jar")

def resolveSQLite():
    run("wget http://central.maven.org/maven2/org/xerial/sqlite-jdbc/3.19.3/sqlite-jdbc-3.19.3.jar")
    run("mv sqlite-jdbc-3.19.3.jar koral/lib/sqlite-jdbc-3.19.3.jar")

def buildKoral():
    with cd("koral"):
        run("ant")
    run("cp koral/build/koral.jar koral.jar");

def installMetis():
    sudo("apt-get update")
    sudo("apt-get -y install metis")
