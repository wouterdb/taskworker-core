Dreamaas Single Machine JDK

This JDK allows experimenting with the Dreamaas API on a single machine. 

It uses the Capedwarf implementation of the Google app engine API, which runs on JBoss AS 7.
For short term storage it uses JBoss' infinispan.
For long term storage it uses Cassandra.


1.Setup
---------------

we assume a linux machine
prerequisites: maven, java, git

1.1.Installing Cassandra
------------------------

We use the datastax packages for cassandra.
For instructions, see http://www.datastax.com/docs/1.1/install/index
For red-hat like systems,as root, do

cat > /etc/yum.repos.d/datastax.repo <<EOF
[datastax]
name= DataStax Repo for Apache Cassandra
baseurl=http://rpm.datastax.com/community
enabled=1
gpgcheck=0
EOF

yum install cassandra12
systemctl start cassandra.service


1.2.Installing the server
-------------------------
Download the patched server form

https://distrinet.cs.kuleuven.be/software/DREAMaaS/jboss-as-7.2.0.Final.tar.gz

extract the archive

start the server with

bin/standalone.sh -c standalone-capedwarf.xml

to clear the cache, do

rm -r standalone/data


1.3.Getting the sdk
---------------------

The sdk and examples can be cloned from github (https://github.com/dreamaas)

more specific, 

git clone git://github.com/dreamaas/taskworker-core.git
pushd taskworker-core
mvn install
popd
git clone git://github.com/dreamaas/taskworker-examples.git
pushd taskworker-examples
mvn install
popd

to deploy the example application, copy the file target/taskworker-0.0.1-SNAPSHOT.war to the server to ./standalone/deployments/ROOT.war
 (e.g. cp target/taskworker-0.0.1-SNAPSHOT.war ../jboss-as/build/target/jboss-as-7.2.0.Final/standalone/deployments/ROOT.war)


1.Rolling your own
------------------
Workers should subclass  drm.taskworker.Worker

