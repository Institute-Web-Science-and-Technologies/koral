#!/bin/bash

# resolve dependency Apache Commons CLI
wget http://apache.mirror.digionline.de//commons/cli/binaries/commons-cli-1.3.1-bin.tar.gz
tar -xf commons-cli-1.3.1-bin.tar.gz
rm commons-cli-1.3.1-bin.tar.gz
mv commons-cli-1.3.1/commons-cli-1.3.1.jar koral/lib/commons-cli-1.3.1.jar
rm -r commons-cli-1.3.1

# resolve dependency JeroMQ
git clone https://github.com/zeromq/jeromq.git
cd jeromq
git checkout tags/v0.3.5
mvn package -DskipTests
cd ..
cp jeromq/target/jeromq-0.3.5.jar koral/lib/jeromq-0.3.5.jar
sudo rm -r jeromq

# resolve dependency Jena
wget https://archive.apache.org/dist/jena/binaries/apache-jena-3.0.0.tar.gz
tar -xf apache-jena-3.0.0.tar.gz
rm apache-jena-3.0.0.tar.gz
mv apache-jena-3.0.0/lib/jena-arq-3.0.0.jar koral/lib/jena-arq-3.0.0.jar
mv apache-jena-3.0.0/lib/jena-base-3.0.0.jar koral/lib/jena-base-3.0.0.jar
mv apache-jena-3.0.0/lib/jena-core-3.0.0.jar koral/lib/jena-core-3.0.0.jar
mv apache-jena-3.0.0/lib/jena-iri-3.0.0.jar koral/lib/jena-iri-3.0.0.jar
mv apache-jena-3.0.0/lib/jena-shaded-guava-3.0.0.jar koral/lib/jena-shaded-guava-3.0.0.jar
mv apache-jena-3.0.0/lib/xercesImpl-2.11.0.jar koral/lib/xercesImpl-2.11.0.jar
mv apache-jena-3.0.0/lib/xml-apis-1.4.01.jar koral/lib/xml-apis-1.4.01.jar
mv apache-jena-3.0.0/lib/slf4j-api-1.7.12.jar koral/lib/slf4j-api-1.7.12.jar
rm -r apache-jena-3.0.0

# discard Jena's logging
wget http://www.slf4j.org/dist/slf4j-1.7.12.tar.gz
tar -xf slf4j-1.7.12.tar.gz
rm slf4j-1.7.12.tar.gz
mv slf4j-1.7.12/slf4j-nop-1.7.12.jar koral/lib/slf4j-nop-1.7.12.jar
rm -r slf4j-1.7.12

# resolve dependency MapDB
git clone https://github.com/jankotek/mapdb.git
cd mapdb
git checkout tags/mapdb-1.0.8
mvn package -DskipTests
cd ..
cp mapdb/target/mapdb-1.0.8.jar koral/lib/mapdb-1.0.8.jar
sudo rm -r mapdb

# resolve Apache Mina FTP Server
wget http://archive.apache.org/dist/mina/ftpserver/1.0.5/ftpserver-1.0.5.zip
unzip ftpserver-1.0.5.zip
rm ftpserver-1.0.5.zip
mv apache-ftpserver-1.0.5/common/lib/ftplet-api-1.0.5.jar koral/lib/ftplet-api-1.0.5.jar
mv apache-ftpserver-1.0.5/common/lib/ftpserver-core-1.0.5.jar koral/lib/ftpserver-core-1.0.5.jar
mv apache-ftpserver-1.0.5/common/lib/mina-core-2.0.0-RC1.jar koral/lib/mina-core-2.0.0-RC1.jar
rm -r apache-ftpserver-1.0.5

# resolve Apache Commons Net
wget https://archive.apache.org/dist/commons/net/binaries/commons-net-3.4-bin.tar.gz
tar -xf commons-net-3.4-bin.tar.gz
rm commons-net-3.4-bin.tar.gz
mv commons-net-3.4/commons-net-3.4.jar koral/lib/commons-net-3.4.jar
rm -r commons-net-3.4

# resolve RocksDB
wget http://central.maven.org/maven2/org/rocksdb/rocksdbjni/4.5.1/rocksdbjni-4.5.1.jar
mv rocksdbjni-4.5.1.jar koral/lib/rocksdbjni-4.5.1.jar
wget http://central.maven.org/maven2/org/osgi/core/4.3.0/core-4.3.0.jar
mv core-4.3.0.jar koral/lib/core-4.3.0.jar
wget http://central.maven.org/maven2/org/xerial/snappy/snappy-java/1.0.3/snappy-java-1.0.3.jar
mv snappy-java-1.0.3.jar koral/lib/snappy-java-1.0.3.jar

# resolve SQLite
wget http://central.maven.org/maven2/org/xerial/sqlite-jdbc/3.8.11.2/sqlite-jdbc-3.8.11.2.jar
mv sqlite-jdbc-3.8.11.2.jar koral/lib/sqlite-jdbc-3.8.11.2.jar
