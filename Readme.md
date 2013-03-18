DREAMaaS Single Machine JDK

This JDK allows experimenting with the DREAMaaS API on a single machine. 

It uses the Capedwarf implementation of the Google app engine API, which runs on JBoss AS 7.
For short term storage it uses JBoss' infinispan.
For long term storage it uses Cassandra.


1.Setup
---------------

we assume a linux machine
prerequisites: maven3 (for Ubuntu, see https://launchpad.net/~natecarlson/+archive/maven3), Java 7, git

1.1.Installing Cassandra
------------------------

We use the datastax packages for cassandra.
For instructions, see http://www.datastax.com/docs/1.1/install/index
For red-hat like systems, as root, do

    cat > /etc/yum.repos.d/datastax.repo <<EOF
    [datastax]
    name= DataStax Repo for Apache Cassandra
    baseurl=http://rpm.datastax.com/community
    enabled=1
    gpgcheck=0
    EOF

    yum install cassandra12
    systemctl start cassandra.service

to make sure cassandra start at boot

    systemctl enable cassandra.service

to see if it runs correctly

    systemctl status cassandra.service



1.2.Installing the server
-------------------------
Download the patched server form

https://distrinet.cs.kuleuven.be/software/DREAMaaS/jboss-as-7.2.0.Final.tar.gz

extract the archive

start the server with

    bin/standalone.sh -c standalone-capedwarf.xml

to clear the cache, do

    rm -r standalone/data


1.3.Getting the DREAMaaS SDK
---------------------

The sdk and examples can be cloned from github (https://github.com/dreamaas)

more specific, 

    git clone git://github.com/dreamaas/taskworker-core.git
    pushd taskworker-core
    mvn clean install
    popd
    git clone git://github.com/dreamaas/taskworker-examples.git
    pushd taskworker-examples
    mvn clean install
    popd

to deploy the example application, go to the directory containing the server and run

    ./bin/jboss-cli.sh
    connect
    deploy --name=ROOT.war [path of dreamaas sdk]/taskworker-examples/target/examples-0.0.1-SNAPSHOT.war

now browse to http://127.0.0.1:8080/


2.Rolling your own
------------------
A dreamaas project consists out of

1- workers

2- configuration


2.1. workers
------------
Workers execute a particular type of Task.
To implement a Worker, subclass drm.taskworker.Worker. When work is available for this worker, the work(Task) method is called.
The Task argument contains the specifics of the work to be done. After all work for a particticular Job is done, the work(EndTask) method is called
Workers should not retain state between different invocations, as multiple instances of each worker may exist, on different machine. All state should be stored in the MemcacheService.

When the work is done, a taskresult is returned. The worker can add new tasks to the taskresult, to order further work to be done. For each task, it must be indicated for what kind of worker it requires. 
To allow modular composition, the name given to each task is first looked up in the configuration file. If it is not found, the name is used directly. In the example code, all work sent out is of the type 'next'. In the configuration file, 'next' is translated to the actual work type. 


2.2. configuration
------------------
the config is expected in the file src/main/webapp/WEB-INF/workers.yaml 

The yaml file has three sections: workers, worklows and scheduler. 
The workers section creates instances of worker classes, binding each to a unique name
The workflow section defines new workflows. Each worklow has the structure

[name]:
    start: [name of start task]
    end: [name of end task]

additionally, a section steps can be used to map abstract task names to actual task names. 

The third section initialisez a scheduler. It requires at least a class argument. All other arguments are passed on to the scheduler itself. 

2.3. Making it work
-------------------
to create a new project, do

    mvn  -e archetype:generate -DarchetypeGroupId=taskworker -DarchetypeArtifactId=worker-archetype -DarchetypeVersion=1.0-SNAPSHOT -DarchetypeRepository=https://distrinet.cs.kuleuven.be/software/DREAMaaS/maven/  -DgroupId=[groud ID] -DartifactId=[project ID]

This will set up a skeleton project.
The project can be built with

    mvn package

To import the project into eclipse use File -> Import -> Existing Maven Project
(If this option  doesn't exist, install the m2e eclipse plugin (help -> eclipse marketplace -> find -> maven integration for eclipse)) 


