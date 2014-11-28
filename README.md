Motivation
----------
As of the time of writing, Hadoop is the major batch-processing platform in
industry. Imho it is important to be able to process data in Cassandra using
Hadoop. The original Cassandra CQLRecordReader works in some important
setups. However, as we are using V-Nodes, it's not working correctly in our setup.

Furthermore, using the original Cassandra-Hadoop InputFormats requires cassandra-all
as a dependency. As MapReduce-programs will just access Cassandra and Hadoop slaves 
won't act as servers a simple Cassandra-client dependency should be enough to run the job.

For these reasons I started a new Cassandra-InputFormat/RecordReader.

Requirements
------------
Install or use a cluster with V-Nnodes, that is compatible with native protocol version
4, i.e. Cassandra 2.1+. It runs on 2.0+, but will display protocol version
downgrade messages.

To write your Hadoop job using Cassandra and this InputFormat please include 
cassandra-hadoop-vnodes-core as a dependency:
'''
<dependency>
  <groupId>scray</groupId>
  <artifactId>cassandra-hadoop-vnodes-core</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
'''

As long as my pull request has not been accepted by datastax, you'll also need to clone
and build https://github.com/AndreasPetter/driver-core

The InputFormat/RecordReader should work with the mapred as well as the mapreduce APIs.

JobConf/Configuration-Parameters
--------------------------------
The following parameters can be configured in your JobConf/Configuration:

* setClusterName(JobConf/Configuration, String) = set the name of the Cassandra cluster (don't know if this has any effect, though)
* setDatacenter(JobConf/Configuration, String) = set the name of the datacenter you want to use the nodes from
* setColumnFamily(JobConf/Configuration, String) = set the name of the column-family for the mappers to read from
* setKeyspace(JobConf/Configuration, String) = set the name of the keyspace for the mappers to read from
* setNodes(JobConf/Configuration, Set<String>) = set IPs/names of Cassandra-nodes
* setNativePort(JobConf/Configuration, int) = set the port to connect to on *every* node
* setWhereClauses(JobConf/Configuration, String) = set additional where information for the CQL-query (may include ALLOW FILTERING)
* setUserName(JobConf/Configuration, String) = set user name for credentials; don't set it if none required
* setUserPwd(JobConf/Configuration, String) = set password for credentials; don't set it if none required

This list should be improved towards completeness. I'll be happy to accept an issue or even better do a pull
request if you want more things to be configurable.

Paging
------
CassandraVNodeRecordReader uses the default auto-paging mechanism of the datastax driver.

Building
--------
Make sure that maven is installed and on your path.
* git clone https://github.com/AndreasPetter/java-driver feature/tokens
* Run ```mvn install``` in the newly created main directory of the Datastax Cassandra Driver
* git clone https://github.com/AndreasPetter/cassandra-hadoop-vnodes
* Run ```mvn install``` in the newly created main directory of cassandra-hadoop-vnodes

Java
----
The InputFormat is written in Scala, because Scala has become one of the major
BigData languages and chances are that your job is already written in Scala. If it's
not: Scala is interoperable with Java, so Java based jobs can be written with
this InputFormat. Only downside is that an up- and downcast is required when assigning
the InputFormat to JobConf, like this:

```job.setInputFormat((Class<InputFormat<Long, Row>>)(Object)CassandraVNodeInputFormat.class);```

(if anyone knows how to circumvent this little inconvenience, I'll be very happy to 
merge in a pull request)

Java-Example
------------
This example counts the rows in a column families in respect to single column.
* Make sure your Hadoop is working by testing with the example Hadoop Jobs in your Hadoop
distribution. Check that your user has the appropriate access rights to your cluster.
* cd into ```cassandra-hadoop-vnodes-examples```
* Edit src/main/java/scray/cassandra/hadoop/example/LineCounter.java. KEYSPACE and COLUMN_FAMILY must 
be set to the name of the keyspace and the column family, in which there are some rows. CASS_HOST should
be set to the hostname or IP of one of your Cassandra nodes. DATA_CENTER should reflect the name of your
configured datacenter in the Cassandra cluster. PARTITION_KEY_COLUMN_NAME should be the name of a column 
for which you want to count (e.g. a partition key).
* Run ```mvn install``` to generate the job-jar in the target directory
* Run ```hadoop jar target/cassandra-hadoop-vnodes-examples-0.0.1-SNAPSHOT-job.jar``` to run the job
* When completed view the output by running ```hadoop fs -cat /tmp/line_counter/part-00000```
* To re-run you must delete the output data first, e.g. ```hadoop fs -rmr /tmp/line_counter```

Cascading, Scalding or Summingbird
----------------------------------
This project provides a storehaus-cascading SplittingMechanism which allows it to be
run with any Cascading job. There are add-ons which in turn allow this stuff to be
used in Scalding (as a Mappable) or in Summingbird (as a Source). Summingburd also
provides means to organize versions.

Set to ```"scray.cassandra.storehaus.CassandraVNodeSplittingMechanism"``` in your
JobConf. This is working with the classic mapred-MapReduce API.

Altough being part of this project the storehaus-module is not being integrated into
the standard build because it has so many additinal dependencies you might not need
at all.

License and Author
------------------
Apache Software License, v 2.0; NO WARRANTIES OF ANY KIND! See: http://www.apache.org/licenses/LICENSE-2.0

Author: Andreas Petter, 2014


