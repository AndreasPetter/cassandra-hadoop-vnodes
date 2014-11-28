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
package scray.cassandra.storehaus

import org.apache.hadoop.mapred.{ InputSplit, JobConf, Reporter }
import com.twitter.storehaus.cascading.InitializableStoreObjectSerializer
import com.twitter.util.Try
import com.twitter.storehaus.cascading.Instance
import com.twitter.storehaus.cascading.split.StorehausSplittingMechanism
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptID
import scala.collection.JavaConversions._
import java.net.InetAddress
import org.slf4j.{ Logger, LoggerFactory }
import scray.cassandra.hadoop.{CassandraVNodeConfigHelper, CassandraVNodeInputFormat, CassandraVNodeRecordReader}
import org.apache.cassandra.hadoop.ColumnFamilySplit
import scray.cassandra.hadoop.CassandraVNodeSplit
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingInitializer

/**
 * SplittingMechanism based on cassandra-hadoop-vnodes-core
 */
class CassandraVNodeSplittingMechanism[K, V, U <: CassandraCascadingInitializer[K, V]](override val conf: JobConf) 
    extends StorehausSplittingMechanism[K, V, U](conf: JobConf) {
  @transient private val log = LoggerFactory.getLogger(classOf[CassandraVNodeSplittingMechanism[K, V, U]])
  
  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  val readVersion = InitializableStoreObjectSerializer.getReadVerion(conf, tapid)
  val storeinit = readVersion match {
    case None => InitializableStoreObjectSerializer.getReadableStoreIntializer(conf, tapid).get.asInstanceOf[CassandraCascadingInitializer[K, V]]
    case Some(version) => InitializableStoreObjectSerializer.getReadableVersionedStoreIntializer(conf, tapid, version).get.
      asInstanceOf[CassandraCascadingInitializer[K, V]]
  } 
  lazy val rowMatcher = storeinit.getCascadingRowMatcher  
  
  override def getSplits(job: JobConf, hint: Int) : Array[InputSplit] = {
    log.debug(s"Getting splits for StorehausTap with id $tapid from Cassandra") 
    val connectionOptions = storeinit.getThriftConnections.trim.split(":").map(s => s.trim)
    CassandraVNodeConfigHelper.setNodes(conf, Set(connectionOptions.head))
    CassandraVNodeConfigHelper.setKeyspace(conf, storeinit.getKeyspaceName)
    CassandraVNodeConfigHelper.setColumnFamily(conf, storeinit.getColumnFamilyName(readVersion))
    CassandraVNodeConfigHelper.setColumns(conf, storeinit.getCascadingRowMatcher.getColumnNamesString)
    CassandraVNodeConfigHelper.setNativePort(conf, storeinit.getNativePort)
    val vnodeFormat = new CassandraVNodeInputFormat
    val splits = Try(vnodeFormat.getSplits(conf, hint)).
      onFailure { e => 
        log.error(s"Got Exception from Cassandra while getting splits on seeds ${ CassandraVNodeConfigHelper.getNodes(conf)}", e)
        throw e
    }
    splits.get.toArray
  }
  
  @transient var recordReader: Option[CassandraVNodeRecordReader] = None
  
  override def initializeSplitInCluster(split: InputSplit, reporter: Reporter): Unit = {
    val storesplit = getVNodeSplit(split)
    // do this again on the mapper, so we can have multiple taps
    CassandraVNodeConfigHelper.setKeyspace(conf, storeinit.getKeyspaceName)
    CassandraVNodeConfigHelper.setColumnFamily(conf, storeinit.getColumnFamilyName(readVersion))
    CassandraVNodeConfigHelper.setColumns(conf, storeinit.getCascadingRowMatcher.getColumnNamesString)
    CassandraVNodeConfigHelper.setNativePort(conf, storeinit.getNativePort)
    CassandraVNodeConfigHelper.setWhereClauses(conf, storeinit.getUserDefinedWhereClauses(readVersion))
    recordReader = Some(new CassandraVNodeRecordReader)
    log.debug(s"Initializing RecordReader for StorehausTap with id $tapid") 
    recordReader.map(_.initialize(split, conf, Some(reporter)))
  }
  
  /**
   * similar to InputSplit.next
   */
  override def fillRecord(split: InputSplit, key: Instance[K], value: Instance[V]): Boolean = {
    val storesplit = getVNodeSplit(split)
    if(recordReader.get.nextKeyValue) {
      val row = recordReader.get.getCurrentValue
      val (cassKey, cassValue) = rowMatcher.getKeyValueFromRow(row)
      log.debug(s"Filling record for StorehausTap with id $tapid with value=$cassValue and key=$cassKey") 
      key.set(cassKey)
      value.set(cassValue)
      true
    } else {
      false
    }
  }
  
  private def getVNodeSplit(split: InputSplit): CassandraVNodeSplit = split.asInstanceOf[CassandraVNodeSplit]
  
  /**
   * free resources after splitting is done
   */
  override def close: Unit = {}

  /**
   * free record reader
   */
  override def closeSplit(split: InputSplit): Unit = recordReader.map(_.close())
}
