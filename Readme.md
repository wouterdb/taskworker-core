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

1.3 Executing the examples
--------------------------

Section 1.2 generates two artifacts (jar and war) which are required to setup a 
working platform.

The war should be deployed as ROOT.war in a servlet container such as Tomcat. 
This war contains a webinterface to submit jobs and inspect the results.

The jar contains the middleware platform and the code of the workers. On each
worker server an instance of this jar should be started. It can be started like this:

java -jar examples.jar workers.yaml

With workers.yaml the configuration file (src/main/webapp/WEB-INF/workers.yaml
in the examples project).

In a distributed setting the workers need at least 3 cassandra instances. To start
a single node development version add the -Ddreamaas.distributed=false parameter.
With -Ddreamaas.cassandra.seed a list of seeds nodes can be given.

The archive worker of the examples also requires the address of an archival services
which is currently included in the war file. For example:
-Ddreamaas.archive.url=http://127.0.0.1:8080/download


2.Rolling your own
------------------

A dreamaas project consists out of

1- workers

2- configuration


2.1. Workers
------------
Workers execute a particular type of Task.
To implement a Worker, subclass drm.taskworker.Worker. When work is available for this worker, the work(Task) method is called.
The Task argument contains the specifics of the work to be done. After all work for a particular Job is done, the work(EndTask) method is called. 
Workers should not retain state between different invocations, as multiple instances of each worker may exist, on different machines. All state should be stored in the MemcacheService.

When the work is done, a taskresult is returned. The worker can add new tasks to the taskresult, to order further work to be done. For each task, it must be indicated what kind of worker it requires. 
To allow modular composition, the name given to each task is first looked up in the configuration file. If it is not found, the name is used directly. In the example code, all work sent out is of the type 'next'. In the configuration file, 'next' is translated to the actual work type. 


2.2. Configuration
------------------
The config is expected in the file src/main/webapp/WEB-INF/workers.yaml 

The yaml file has three sections: workers, worklows and scheduler. 
The workers section creates instances of worker classes, binding each to a unique name
The workflow section defines new workflows. Each worklow has the following structure

    [name]:
       start: [name of start task]
       end: [name of end task]

additionally, a section step can be used to map abstract task names to actual task names. 

The third section initializes a scheduler. It requires at least a class argument. All other arguments are passed on to the scheduler itself. 



