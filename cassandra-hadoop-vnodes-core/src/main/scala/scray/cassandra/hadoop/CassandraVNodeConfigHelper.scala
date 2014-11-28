// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.cassandra.hadoop

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.conf.Configuration
import java.util.{Set => JSet}
import scala.collection.JavaConverters._

/**
 * Some helper to set configuration parameters.
 * The number of config parameters is intentionally small to make the config easier to 
 * understand and use. Please suggest some more if you need them (or issue a PR). 
 */
object CassandraVNodeConfigHelper {
  val CLUSTER_NAME = "scray.cassandra.cluster" // JobConf-param name of the Cassandra cluster
  val DATACENTER = "scray.cassandra.datacenter" // JobConf-param name of the DC
  val COLUMN_FAMILY = "scray.cassandra.columnfamily" // JobConf-param of the column family
  val COLUMNS = "scray.cassandra.columns" // JobConf-param of the columns to be used in the query
  val KEYSPACE = "scray.cassandra.keyspace" // JobConf-param of the keyspace
  val NODES = "scray.cassandra.nodes" // JobConf-param of the Cassandra-nodes to connect to
  val NATIVE_PORT = "scray.cassandra.native.port" // JobConf-param of the Cassandra-port to connect to
  val WHERE_CLAUSES = "scray.cassandra.where" // JobConf-param to specify additional a where clause
  val USER_NAME = "scray.cassandra.user.name" // JobConf-param to specify username for Cassandra-credentials
  val USER_PWD = "scray.cassandra.user.pwd" // JobConf-param to specify password for Cassandra-credentials
    
  val IGNORE_MARKER_VALUE = "-ignored-"
  val DEFAULT_NATIVE_PORT = 9042
  
  val COMMA = ";"
  
  /* mapred API */
  def setClusterName(conf: JobConf, cluster: String): Unit = conf.set(CLUSTER_NAME, cluster)
  def setDatacenter(conf: JobConf, dc: String): Unit = conf.set(DATACENTER, dc)
  def setColumnFamily(conf: JobConf, cf: String): Unit = conf.set(COLUMN_FAMILY, cf)
  def setColumns(conf: JobConf, columns: String): Unit = conf.set(COLUMN_FAMILY, columns)
  def setKeyspace(conf: JobConf, keyspace: String): Unit = conf.set(KEYSPACE, keyspace)
  def setNodes(conf: JobConf, hosts: JSet[String]): Unit = conf.set(NODES, hosts.asScala.mkString(COMMA))
  def setNativePort(conf: JobConf, port: Int): Unit = conf.set(NATIVE_PORT, port.toString)
  def setWhereClauses(conf: JobConf, clauses: String): Unit = conf.set(WHERE_CLAUSES, clauses)
  def setUserName(conf: JobConf, user: String): Unit = conf.set(USER_NAME, user)
  def setUserPwd(conf: JobConf, pwd: String): Unit = conf.set(USER_PWD, pwd)
  
  def getClusterName(conf: JobConf): String = Option(conf.get(CLUSTER_NAME)).getOrElse(IGNORE_MARKER_VALUE)
  def getDatacenter(conf: JobConf): String = Option(conf.get(DATACENTER)).getOrElse(IGNORE_MARKER_VALUE)
  def getColumnFamily(conf: JobConf): String = conf.get(COLUMN_FAMILY)
  def getColumns(conf: JobConf): String = Option(conf.get(COLUMNS)).getOrElse("*")
  def getKeyspace(conf: JobConf): String = conf.get(KEYSPACE)
  def getNodes(conf: JobConf): Set[String] = Option(conf.get(NODES)).map { hosts =>
    hosts.split(COMMA).toSet
  }.getOrElse(Set("localhost"))
  def getNativePort(conf: JobConf): Int = Option(conf.get(NATIVE_PORT)).
    map(Integer.parseInt(_)).getOrElse(DEFAULT_NATIVE_PORT)
  def getWhereClauses(conf: JobConf): String = Option(conf.get(WHERE_CLAUSES)).getOrElse("")
  def getUserName(conf: JobConf): String = conf.get(USER_NAME)
  def getUserPwd(conf: JobConf): String = conf.get(USER_PWD)
  
  /* mapreduce API */
  def setClusterName(conf: Configuration, cluster: String): Unit = conf.set(CLUSTER_NAME, cluster)
  def setDatacenter(conf: Configuration, dc: String): Unit = conf.set(DATACENTER, dc)
  def setColumnFamily(conf: Configuration, cf: String): Unit = conf.set(COLUMN_FAMILY, cf)
  def setColumns(conf: Configuration, columns: String): Unit = conf.set(COLUMN_FAMILY, columns)
  def setKeyspace(conf: Configuration, keyspace: String): Unit = conf.set(KEYSPACE, keyspace)
  def setNodes(conf: Configuration, hosts: JSet[String]): Unit = conf.set(NODES, hosts.asScala.mkString(COMMA))
  def setNativePort(conf: Configuration, port: Int): Unit = conf.set(NATIVE_PORT, port.toString)
  def setWhereClauses(conf: Configuration, clauses: String): Unit = conf.set(WHERE_CLAUSES, clauses)
  def setUserName(conf: Configuration, user: String): Unit = conf.set(USER_NAME, user)
  def setUserPwd(conf: Configuration, pwd: String): Unit = conf.set(USER_PWD, pwd)
  
  def getClusterName(conf: Configuration): String = Option(conf.get(CLUSTER_NAME)).getOrElse(IGNORE_MARKER_VALUE)
  def getDatacenter(conf: Configuration): String = Option(conf.get(DATACENTER)).getOrElse(IGNORE_MARKER_VALUE)
  def getColumnFamily(conf: Configuration): String = conf.get(COLUMN_FAMILY)
  def getColumns(conf: Configuration): String = Option(conf.get(COLUMNS)).getOrElse("*")
  def getKeyspace(conf: Configuration): String = conf.get(KEYSPACE)
  def getNodes(conf: Configuration): Set[String] = Option(conf.get(NODES)).map { hosts =>
    hosts.split(COMMA).toSet
  }.getOrElse(Set("localhost"))
  def getNativePort(conf: Configuration): Int = Option(conf.get(NATIVE_PORT)).
    map(Integer.parseInt(_)).getOrElse(DEFAULT_NATIVE_PORT)
  def getWhereClauses(conf: Configuration): String = Option(conf.get(WHERE_CLAUSES)).getOrElse("")
  def getUserName(conf: Configuration): String = conf.get(USER_NAME)
  def getUserPwd(conf: Configuration): String = conf.get(USER_PWD) 
}
