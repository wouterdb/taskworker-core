# DREAMaaS Middleware SDK

This SDK allows experimenting with the DREAMaaS API on a cluster.

## 1.Setup

We assume a linux machine with a recent distribution.

Prerequisites: Java 7, git, maven3 (For Ubuntu, see https://launchpad.net/~natecarlson/+archive/maven3. The command will be then 'mvn3' instead of 'mvn'.)

### 1.1. Installing Cassandra 1.2

We use the datastax packages for cassandra.
For instructions, see http://www.datastax.com/docs/1.2/install/index
For red-hat like systems, as root, do

```bash
cat > /etc/yum.repos.d/datastax.repo <<EOF
[datastax]
name= DataStax Repo for Apache Cassandra
baseurl=http://rpm.datastax.com/community
enabled=1
gpgcheck=0
EOF

yum install cassandra12
systemctl start cassandra.service
```

to make sure cassandra start at boot

```bash
systemctl enable cassandra.service
```

to see if it runs correctly

```bash
systemctl status cassandra.service
```

The distributed version requires a cluster of at least three Cassandra servers


### 1.2. Installing the DREAMaaS SDK

The sdk can be cloned from github (https://github.com/dreamaas). The repository
is called taskworker-core. Checking out the repository and compiling the code can 
be done with the following commands:

```bash
git clone git://github.com/dreamaas/taskworker-core.git
cd taskworker-core
mvn clean install
```

This generates a jar in the target directory with all dependencies included. 

The mvn install command also installs the generated artifacts in your local
repository.

In target a tar.bz2 file of the project is created, in conjunction with the 
spec file in the root, an rpm file can be created for recent fedora versions:

```bash
cp target/taskworker-core.tar.bz2 ~/rpmbuild/SOURCES/
rpmbuild -bb taskworker-core.spec
```
    
For a manual install:

*   Copy the generated jar to /usr/share/java
    > cp target/taskworker-core.jar /usr/share/java
    
*   Copy the two executable scripts to /usr/bin or /usr/local/bin
    > cp bin/taskworker-server bin/taskworker-client /usr/bin
    
*   Create the configuration directory
    > mkdir /etc/taskworker
    
*   Properties can be overridden by defining them in /etc/taskworker/config.properties

Note: In the taskworker-server script the location of the properties file
and the jar can be changed.

### 1.3 Starting taskworker server and execute a workflow

The taskworker-workers repository contains the code for several workers and
integration projects that generate jar with all the worker code and have a
configuration file that defines the workers, the schedular config and the workflows.

The /etc/taskworker/config.properties properties file should define the location
of the workflow configuration file (yaml file format) in the property
taskworker.configfile

The properties file determines which components of the server are started:

*   workers: The workers defined and used the workflows

*   scheduler: A scheduler that starts jobs by moving them onto the work queue.
Only one scheduler per "cluster" must be started.

*   rest: A rest interface to submit jobs and track their progress.  

By default the middleware starts a non-distributed version which uses local
locking only. In a distributed setting the workers need at least 3 cassandra 
instances to ensure quorum is achieved when a lock is requested. To start
the distirbutedment version set taskworker.distributed property to true.

The taskworker-client script is a Python script that uses the built-in REST
interface to interact with the taskworker-server.

