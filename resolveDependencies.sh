#!/bin/bash

# resolve dependency Apache Commons CLI
wget http://artfiles.org/apache.org//commons/cli/binaries/commons-cli-1.4-bin.tar.gz
tar -xf commons-cli-1.4-bin.tar.gz
rm commons-cli-1.4-bin.tar.gz
mv commons-cli-1.4/commons-cli-1.4.jar koral/lib/commons-cli-1.4.jar
rm -r commons-cli-1.4

# resolve dependency JeroMQ
wget http://central.maven.org/maven2/org/zeromq/jeromq/0.4.0/jeromq-0.4.0.jar
mv jeromq-0.4.0.jar koral/lib/jeromq-0.4.0.jar

# resolve dependency Jena
wget https://archive.apache.org/dist/jena/binaries/apache-jena-3.3.0.tar.gz
tar -xf apache-jena-3.3.0.tar.gz
rm apache-jena-3.3.0.tar.gz
mv apache-jena-3.3.0/lib/jena-arq-3.3.0.jar koral/lib/jena-arq-3.3.0.jar
mv apache-jena-3.3.0/lib/jena-base-3.3.0.jar koral/lib/jena-base-3.3.0.jar
mv apache-jena-3.3.0/lib/jena-core-3.3.0.jar koral/lib/jena-core-3.3.0.jar
mv apache-jena-3.3.0/lib/jena-iri-3.3.0.jar koral/lib/jena-iri-3.3.0.jar
mv apache-jena-3.3.0/lib/jena-shaded-guava-3.3.0.jar koral/lib/jena-shaded-guava-3.3.0.j ar
mv apache-jena-3.3.0/lib/xercesImpl-2.11.0.jar koral/lib/xercesImpl-2.11.0.jar
mv apache-jena-3.3.0/lib/xml-apis-1.4.01.jar koral/lib/xml-apis-1.4.01.jar
mv apache-jena-3.3.0/lib/slf4j-api-1.7.21.jar koral/lib/slf4j-api-1.7.21.jar
rm -r apache-jena-3.3.0

# discard Jena's logging
wget http://www.slf4j.org/dist/slf4j-1.7.25.tar.gz
tar -xf slf4j-1.7.25.tar.gz
rm slf4j-1.7.25.tar.gz
mv slf4j-1.7.25/slf4j-nop-1.7.25.jar koral/lib/slf4j-nop-1.7.25.jar
rm -r slf4j-1.7.25

# resolve dependency MapDB
wget http://central.maven.org/maven2/org/mapdb/mapdb/3.0.4/mapdb-3.0.4.jar
mv mapdb-3.0.4.jar koral/lib/mapdb-3.0.4.jar

# resolve Apache Mina FTP Server
wget http://archive.apache.org/dist/mina/ftpserver/1.1.1/ftpserver-1.1.1.zip
unzip ftpserver-1.1.1.zip
rm ftpserver-1.1.1.zip
mv apache-ftpserver-1.1.1/common/lib/ftplet-api-1.1.1.jar koral/lib/ftplet-api-1.1.1.jar
mv apache-ftpserver-1.1.1/common/lib/ftpserver-core-1.1.1.jar koral/lib/ftpserver-core-1.1.1.jar
mv apache-ftpserver-1.1.1/common/lib/mina-core-2.0.16.jar koral/lib/mina-core-2.0.16.jar
rm -r apache-ftpserver-1.1.1

# resolve Apache Commons Net
wget https://archive.apache.org/dist/commons/net/binaries/commons-net-3.6-bin.tar.gz
tar -xf commons-net-3.6-bin.tar.gz
rm commons-net-3.6-bin.tar.gz
mv commons-net-3.6/commons-net-3.6.jar koral/lib/commons-net-3.6.jar
rm -r commons-net-3.6

# resolve RocksDB
wget http://central.maven.org/maven2/org/rocksdb/rocksdbjni/5.4.5/rocksdbjni-5.4.5.jar
mv rocksdbjni-5.4.5.jar koral/lib/rocksdbjni-5.4.5.jar
wget http://central.maven.org/maven2/org/osgi/org.osgi.core/6.0.0/org.osgi.core-6.0.0.jar
mv org.osgi.core-6.0.0.jar koral/lib/org.osgi.core-6.0.0.jar
wget http://central.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.4/snappy-java-1.1.4.jar
mv snappy-java-1.1.4.jar koral/lib/snappy-java-1.1.4.jar

# resolve SQLite
wget http://central.maven.org/maven2/org/xerial/sqlite-jdbc/3.19.3/sqlite-jdbc-3.19.3.jar
mv sqlite-jdbc-3.19.3.jar koral/lib/sqlite-jdbc-3.19.3.jar
