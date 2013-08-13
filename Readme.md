DREAMaaS Middleware SDK

This SDK allows experimenting with the DREAMaaS API on a cluster.

1.Setup
---------------

we assume a linux machine
prerequisites: Java 7, git, maven3 (For Ubuntu, see https://launchpad.net/~natecarlson/+archive/maven3. The command will be then 'mvn3' instead of 'mvn'.)

1.1.Installing Cassandra 1.2
----------------------------

We use the datastax packages for cassandra.
For instructions, see http://www.datastax.com/docs/1.2/install/index
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

The distributed version requires a cluster of at least three Cassandra servers


1.2.Getting the DREAMaaS SDK
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

This generates two artifacts:
    - a jar that contains the platform and the workers that can be started from
      the commandline.
    - a war that contains the example servlets that needs to be installed in a tomcat

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

The third section initialises a scheduler. It requires at least a class argument. All other arguments are passed on to the scheduler itself. 

2.3. Making it work
-------------------
To create a new project, do

    mvn -e archetype:generate -DarchetypeGroupId=taskworker -DarchetypeArtifactId=worker-archetype -DarchetypeVersion=1.0-SNAPSHOT -DarchetypeRepository=https://distrinet.cs.kuleuven.be/software/DREAMaaS/maven/  -DgroupId=[groud ID] -DartifactId=[project ID]

This will set up a skeleton project.
The project can be built with

    mvn package

To import the project into eclipse use File -> Import -> Existing Maven Project
(If this option  doesn't exist, install the m2e eclipse plugin (help -> eclipse marketplace -> find -> maven integration for eclipse)) 

3. Remarks
-----------

For remarks, please use the issue tracker

