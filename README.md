# Koral

A fast-forward distributed RDF store aiming to be independent of the underlying data placement strategy. It is tested with Ubuntu 14.04 and Orace Java JDK 1.8.

## Building Koral Locally

In order to build Koral locally, you have to install the following programs: Oracle JDK 1.8, git, ant, unzip, maven and metis. In order to install Oracle JDK 1.8 you can execute:
```
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
sudo apt-get -y install oracle-java8-installer
sudo apt-get -y install oracle-java8-set-default
```

The other packages can be installed by:
```
sudo apt-get -y install git ant unzip maven metis
```

Now, you can clone the Koral git repository
```
git clone https://github.com/Institute-Web-Science-and-Technologies/koral.git
```
and build Koral
```
/bin/bash koral/resolveDependencies.sh
cd koral
ant
cd ..
```
The built JAR can be found under `koral/build/koral.jar`.

## Deploying Koral in a Cluster

Since installing Koral on several computers is time-consuming, some Fabric scripts are provided that help to deploy, start and stop a Koral cluster.

### Configuring Koral

In order to run Koral, the different masters and slaves have to be configured properly. Therefore, a configuration template could be found at `koral/koralConfig.xml.template`. You can create your own configuration file by copying this template `cp koral/koralConfig.xml.template koralConfig.xml` and then editing its content. A description of the different configurable properties is contained in the file.

You should at least configure the following properties:
- master: the IP address which the slaves use to contact the master
- slaves: a comma separated list of the IPs of the slaves
- clientConnection: the IP of the server to which the clients can connect
- ftpServer: the IP address the client will use to upload the graph files
You can add, e.g. `:1234`, to each IP address to specify the used port. If you leave it out, the default ports will be used.

Furthermore, you can specify the different directory where Koral stores data. The master and the slaves use the following directories:
- loggingDirectory: the directory where the log files are written to. Default: `.`
- tmpDir: the directory where intermediate data is stored. Default: `/tmp`

Only the master uses the following directories:
- dictionaryDir: the directory where the dictionary is created in. Default: `./dictionary`
- statisticsDir: the directory where the statistics database is created in. Default: `./statistics`

Only the slaves use the following directory:
- tripleStoreDir: the directory where the local triple indices are created in. Default: `./tripleStore`

### Setting up Fabric

In order to deploy Koral, we use [Fabric](http://www.fabfile.org/) scripts to deploy and manage Koral remotely. You can install Fabric by executing:
```
sudo apt-get install fabric
```

The scripts connect to the different remote machines via SSH. Before you can use the scripts, you need to configure the addresses of the master and the slaves and how you can authorize on them. First, you should create a copy of `koral\scripts\environment.py.template` by executing
```
cp koral\scripts\environment.py.template koral\scripts\environment.py
```

Now, you can enter the IP addresses of the master and the slaves at the definition of `env.roledefs`. You should define the user name which will be used to access the remote machines. Define it with the property `env.user`. Basically, the user on the remote machines should have password-less sudo access to prevent typing the sudo password several times.

When configuring the SSH authentication, you have two option: (i) password-based or (ii) a public key-based authentication:
1. For the password-based authentication define it with the property `env.password`.
2. For the public key-based authentication define the path to the private key with the property `env.key_filename`. You should use the absolute path.

You can check whether everything is set up correctly by executing:
```
cd koral/scripts
fab -f koral.py test
cd ../..
```
It should connect to the master and the slaves and print a success message on the screen.

If the remote machines have to be set up first, e.g. updating the operating system, you can declare the specific commands in the body of method `initializeVM()`. Basically a user command can be executed by specifying `run("enter command here")`. A sudo command can be executed by `sudo("enter command here")`. For further details have a look at [Fabric's API documentation](http://docs.fabfile.org/).

### Installing Koral

When you have set up Fabric you can execute the following command to install Koral on the complete cluster:
```
cd koral/scripts
fab -f koral.py install
cd ../..
```

### Managing Koral Cluster

When you want to update the configuration file of the Koral cluster, execute the following command:
```
cd koral/scripts
fab -f koral.py updateConfig:"/path/to/koralConfig.xml"
cd ../..
```

If you want to start the cluster (remember to update the configuration if necessary in beforehand), you can execute:
```
cd koral/scripts
fab -f koral.py start
cd ../..
```

You can stop the cluster, by executing:
```
cd koral/scripts
fab -f koral.py stop
cd ../..
```

#### Collecting Log Messages

If you want to execute a Koral cluster and you want to see all log messages during the runtime of Koral without logging in on all machines, we provide the option, that you can start a log message receiver locally and tell Koral to send all log messages additionally to your local log message receiver.

You start the local log message receiver by executing:
```
java -jar koral/build/koral.jar logReceiver
```
Optionally, you can pass the local IP to bind to via the argument `-i yourIP` and the used port via the argument `-p yourPort`. The `-p` is missing the default port 4712 is used.

You can instruct Koral to send the local messages to your log receiver by starting it with the following command:
```
cd koral/scripts
fab -f koral.py start:remoteLogger=yourIP
cd ../..
```
or if you are using a different port
```
cd koral/scripts
fab -f koral.py start:remoteLogger=yourIP:yourPort
cd ../..
```

#### Activating Runtime Measurements

You can also instruct Koral to measure runtime metrics. The measured metrics are sent to a measurement receiver.

You can start the measurement receiver by executing:
```
java -jar koral/build/koral.jar measurementReceiver -o /output/file/for/measurements.csv.gz
```
Optionally, you can pass the local IP to bind to via the argument `-i yourIP` and the used port via the argument `-p yourPort`. The `-p` is missing the default port 4713 is used.

You can instruct Koral to collect and send measurements by starting it with the following command:
```
cd koral/scripts
fab -f koral.py start:remoteMeasurementCollector=yourIP
cd ../..
```
or if you are using a different port
```
cd koral/scripts
fab -f koral.py start:remoteMeasurementCollector=yourIP:yourPort
cd ../..
```

## Interacting with Koral

You can send the following commands to Koral with its client.

### Load a dataset

You can load a dataset by executing
```
java -jar koral/build/koral.jar client -m IPofMaster load -c HASH -n 0 /path/to/graphFile.rdf
```

- With the argument `-m` you specify the IP of the master and optionally its port.
- With the argument `-c` you specify the graph cover strategy to be applied on the RDF graph. Possible values are: HASH, HIERARCHICAL and MIN\_EDGE\_CUT 
- With the argument `-n` you specify the length of the n-hop replication. A value of 0 means no n-hop replication.
- At the end you can define a folder containing several graph files or a whitespace separated list of individual graph files. These graphs are loaded.

### Execute a query

```
java -jar koral/build/koral.jar client -m IPofMaster query -t BUSHY -o /path/to/resultFile.csv /path/to/queryFile.sparql
```

- With the argument `-m` you specify the IP of the master and optionally its port.
- With the argument `-t` specifying the query execution tree type. Possible values are BUSHY, LEFT\_LINEAR, RIGHT\_LINEAR. The default value is LEFT\_LINEAR
- With the argument `-o` specifying the file to which the query results are written to. If this argument is not given the results are printed to the standard output.
- At the end the file containing the query is specified that should be executed.

### Drop the database

```
java -jar koral/build/koral.jar client -m IPofMaster drop
```

- With the argument `-m` you specify the IP of the master and optionally its port.

## Extending Koral

You can extend Koral by your own graph cover strategies. Therefore, you have to implement the interface `de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator`. Then, you add a new constant for your cover strategy to the enumeration `de.uni_koblenz.west.koral.master.graph_cover_creator.CoverStrategyType`. This name will be automatically be selectable via the command line when loading a graph. Finally, you add an entry in the factory class `de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreatorFactory` so that a new instance of your graph cover creator class is created when it is selected during the loading process.