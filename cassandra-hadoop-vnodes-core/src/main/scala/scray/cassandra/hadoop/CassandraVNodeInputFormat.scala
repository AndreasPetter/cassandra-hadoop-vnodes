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

import org.apache.hadoop.mapred.{ InputFormat, InputSplit, JobConf, Reporter, RecordReader }
import org.apache.hadoop.mapreduce.{ InputFormat => NewInputFormat, InputSplit => NewInputSplit, RecordReader => NewRecordReader, TaskAttemptContext }
import java.lang.{ Long => JLong }
import java.util.{ List => JList }
import com.datastax.driver.core.{Cluster, Row, Session}
import org.slf4j.LoggerFactory
import org.apache.hadoop.mapreduce.JobContext
import scala.collection.JavaConverters._

/**
 * An Hadoop-InputFormat that is only using a modified Datastax Java-Driver
 * to produce splits.
 */
class CassandraVNodeInputFormat extends NewInputFormat[JLong, Row] with InputFormat[JLong, Row] {

  @transient val logger =  LoggerFactory.getLogger(classOf[CassandraVNodeInputFormat])
  
  /**
   * Connects to some node and reads all possible tokens for their nodes 
   * in a provided DC and stores them in corresponding CassandraVNodeSplits.
   */
  override def getSplits(conf: JobConf, hint: Int): Array[InputSplit] = {
    // setup cluster
    val hosts = CassandraVNodeConfigHelper.getNodes(conf)
    val port = CassandraVNodeConfigHelper.getNativePort(conf)
    val credentials = CassandraVNodeTools.getCredentialsOption(conf)
    val keyspacename = CassandraVNodeConfigHelper.getKeyspace(conf)
    val dc = CassandraVNodeConfigHelper.getDatacenter(conf) match {
      case CassandraVNodeConfigHelper.IGNORE_MARKER_VALUE => None
      case x:String => Some(x)
    }
    logger.debug(s"Getting splits from Cassandra-Cluster with hosts $hosts on keyspace $keyspacename")

    val cluster = CassandraVNodeTools.getCluster(hosts, port, credentials)
    try {
      // setup Session
      val session = cluster.connect(keyspacename)
      // get list of token ranges; must cast because array is invariant
      CassandraVNodeTools.getSplitsForKeyspace(cluster, keyspacename, dc).asInstanceOf[Array[InputSplit]]
    } finally {
      // shutdown cluster on client
      cluster.closeAsync
    }
  }

  override def getSplits(context: JobContext): JList[NewInputSplit] =
    getSplits(CassandraVNodeConfigurationConverter(context.getConfiguration()), 0).map(_.asInstanceOf[NewInputSplit]).toList.asJava
  
  /**
   * creates a RecordReader from the InputSplit
   */
  override def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter): RecordReader[JLong, Row] =
    new CassandraVNodeRecordReader().initialize(split, conf, Some(reporter))    
  override def createRecordReader(split: NewInputSplit, tac: TaskAttemptContext): NewRecordReader[JLong, Row] = new CassandraVNodeRecordReader()
    
}
