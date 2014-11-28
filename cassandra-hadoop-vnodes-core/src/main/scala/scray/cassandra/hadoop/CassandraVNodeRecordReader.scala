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

import com.datastax.driver.core.{ Cluster, ColumnDefinitions, Row, Session, TupleValue, UDTValue }
import com.datastax.driver.core.policies.{ Policies, WhiteListPolicy }
import java.lang.{ Long => JLong }
import java.math.{ BigDecimal, BigInteger }
import java.net.{ InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.util.{ Date, UUID, List => JList, Map => JMap, Set => JSet }
import org.apache.hadoop.mapred.{ InputSplit, JobConf, RecordReader, Reporter }
import org.apache.hadoop.mapreduce.{ InputSplit => NewInputSplit, RecordReader => NewRecordReader, TaskAttemptContext }
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * RecordReader to read from Cassandra using a CassandraVNodeSplit
 */
class CassandraVNodeRecordReader extends NewRecordReader[JLong, Row] with RecordReader[JLong, Row] {

  val logger = LoggerFactory.getLogger(classOf[CassandraVNodeRecordReader])
  
  var cluster: Option[Cluster] = None
  var session: Option[Session] = None
  var rows: Option[Iterator[Row]] = None
  var reporter: Option[Reporter] = None
  var counter = -1L

  /**
   * mapreduce initialize call mapred initialize:
   *  - CassandraVNodeSplit implements both apis
   *  - Configuration can be converted
   */
  override def initialize(split: NewInputSplit, tac: TaskAttemptContext): Unit = {
    initialize(split.asInstanceOf[InputSplit], CassandraVNodeConfigurationConverter(tac.getConfiguration()), None)
  }
  
  /**
   * initialize this record reader for the first and only time
   */
  def initialize(split: InputSplit, conf: JobConf, rep: Option[Reporter]): CassandraVNodeRecordReader = {
    rep.map(_.setStatus("INITIALIZING"))
    val csplit = split.asInstanceOf[CassandraVNodeSplit]
    val port = CassandraVNodeConfigHelper.getNativePort(conf)
    val credentials = CassandraVNodeTools.getCredentialsOption(conf)
    val keyspacename = CassandraVNodeConfigHelper.getKeyspace(conf)
    val dc = CassandraVNodeConfigHelper.getDatacenter(conf) match {
      case CassandraVNodeConfigHelper.IGNORE_MARKER_VALUE => None
      case x:String => Some(x)
    }
    logger.debug(s"CassandraVNodeRecordReader is trying to connect to default/nearest host, ${csplit.getDefaultHost}")
    // if the connecting to the main host fails, we subsequently connect to one of the other replicas
    val socket = Set(new InetSocketAddress(InetAddress.getByName(csplit.getDefaultHost), port)).asJava
    val allsockets = csplit.getAllHosts.filterNot(_ == csplit.getDefaultHost).
      map(hostip => new InetSocketAddress(InetAddress.getByName(hostip), port)).asJava
    val whitelistSingle = new WhiteListPolicy(Policies.defaultLoadBalancingPolicy(), socket)
    val whitelistAll = new WhiteListPolicy(Policies.defaultLoadBalancingPolicy(), allsockets)
    cluster = Some(Try(CassandraVNodeTools.getCluster(Set(csplit.getDefaultHost), port, credentials, whitelistSingle)).
      getOrElse {
        logger.info(s"CassandraVNodeRecordReader could not connect to default/nearest host, trying other replicas")
        CassandraVNodeTools.getCluster(csplit.getAllHosts, port, credentials, whitelistAll)
    })
    session = Some(cluster.get.connect(keyspacename))
    
    // execute the query
    rows = Some(session.get.execute(getCQLQuery(csplit, conf)).iterator().asScala)
    
    rep.map(_.setStatus("RUNNING"))
    reporter = rep
    logger.debug(s"CassandraVNodeRecordReader is ready to iterate over Rows")
    this
  }

  /**
   * creates a CQL query comprising the token ands the additional where clauses
   * provided via jobconf.
   */
  private def getCQLQuery(split: CassandraVNodeSplit, conf: JobConf): String = {
    val SEMICOLON = ";"
    val sb = new StringBuffer("SELECT ")
    sb.append(CassandraVNodeConfigHelper.getColumns(conf))
    sb.append(" FROM ")
    sb.append(CassandraVNodeConfigHelper.getColumnFamily(conf))
    sb.append(" WHERE")
    split.getStartToken.map(token => sb.append(s" TOKEN(key) >= $token"))
    if(split.getStartToken.isDefined && split.getEndToken.isDefined) {
      sb.append(" AND")
    }
    split.getEndToken.map(token => sb.append(s" TOKEN(key) < $token"))
    if(CassandraVNodeConfigHelper.getWhereClauses(conf) != "") {
      val trimmed = CassandraVNodeConfigHelper.getWhereClauses(conf).trim
      if(!trimmed.substring(0, 3).equalsIgnoreCase("and")) {
        sb.append(" AND ")
      } else {
        sb.append(" ")
      }
      sb.append(trimmed)
      if(!trimmed.endsWith(SEMICOLON)) {
        sb.append(SEMICOLON)
      }
    } else {
      sb.append(SEMICOLON)
    }
    logger.info(s"Using CQL-Query-String ${sb.toString}")
    sb.toString
  }
  
  override def nextKeyValue: Boolean = rows.get.hasNext
  
  override def getCurrentValue: Row = {
    counter += 1
    rows.get.next
  }
  
  override def getCurrentKey: JLong = counter
  
  override def next(key: JLong, value: Row): Boolean = nextKeyValue match {
    case true => value.asInstanceOf[WrappedRow].setRow(getCurrentValue)
      true
    case _ => false
  } 

  override def createKey: JLong = 0L
  override def getProgress: Float = 0.0F
  override def getPos: Long = counter
  override def createValue: Row = new WrappedRow(None)
  override def close: Unit = {
    logger.info(s"Pulled ${getPos + 1L} rows from CassandraVNodeRecordReader")
    reporter.map(_.setStatus("CLOSING"))
    session.map(_.close)
    cluster.map(_.close)
    reporter.map(_.setStatus("COMPLETED"))
  }
  
  /**
   * similar to the solution of the original WrappedRow in Cassandra-code
   * this is a wrapper for the row to cope with the mapred API
   */
  class WrappedRow(var row: Option[Row]) extends Row {
    def setRow(r: Row): Unit = {
      row = Some(r)
    }
    override def getColumnDefinitions: ColumnDefinitions = row.get.getColumnDefinitions
    override def isNull(i: Int): Boolean = row.get.isNull(i)
    override def isNull(name: String): Boolean = row.get.isNull(name)
    override def getBool(i: Int): Boolean = row.get.getBool(i)
    override def getBool(name: String): Boolean = row.get.getBool(name)
    override def getInt(i: Int): Int = row.get.getInt(i)
    override def getInt(name: String): Int = row.get.getInt(name)
    override def getLong(i: Int): Long = row.get.getLong(i)
    override def getLong(name: String): Long = row.get.getLong(name)
    override def getDate(i: Int): Date = row.get.getDate(i)
    override def getDate(name: String): Date = row.get.getDate(name)
    override def getFloat(i: Int): Float = row.get.getFloat(i)
    override def getFloat(name: String): Float = row.get.getFloat(name)
    override def getDouble(i: Int): Double = row.get.getDouble(i)
    override def getDouble(name: String): Double = row.get.getDouble(name)
    override def getBytesUnsafe(i: Int): ByteBuffer = row.get.getBytesUnsafe(i)
    override def getBytesUnsafe(name: String): ByteBuffer = row.get.getBytesUnsafe(name)
    override def getBytes(i: Int): ByteBuffer = row.get.getBytes(i)
    override def getBytes(name: String): ByteBuffer = row.get.getBytes(name)
    override def getString(i: Int): String = row.get.getString(i)
    override def getString(name: String): String = row.get.getString(name)
    override def getVarint(i: Int): BigInteger = row.get.getVarint(i)
    override def getVarint(name: String): BigInteger = row.get.getVarint(name)
    override def getDecimal(i: Int): BigDecimal = row.get.getDecimal(i)
    override def getDecimal(name: String): BigDecimal = row.get.getDecimal(name)
    override def getUUID(i: Int): UUID = row.get.getUUID(i)
    override def getUUID(name: String): UUID = row.get.getUUID(name)
    override def getInet(i: Int): InetAddress = row.get.getInet(i)
    override def getInet(name: String): InetAddress = row.get.getInet(name)
    override def getList[T](i: Int, elementsClass: Class[T]): JList[T] = row.get.getList(i, elementsClass)
    override def getList[T](name: String, elementsClass: Class[T]): JList[T] = row.get.getList(name, elementsClass)
    override def getSet[T](i: Int, elementsClass: Class[T]): JSet[T] = row.get.getSet(i, elementsClass)
    override def getSet[T](name: String, elementsClass: Class[T]): JSet[T] = row.get.getSet(name, elementsClass)
    override def getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V]): JMap[K, V] =
      row.get.getMap(i, keysClass, valuesClass)
    override def getMap[K, V](name: String, keysClass: Class[K], valuesClass: Class[V]): JMap[K, V] = 
      row.get.getMap(name, keysClass, valuesClass)
    override def getTupleValue(i: Int): TupleValue = row.get.getTupleValue(i)
    override def getTupleValue(name: String): TupleValue = row.get.getTupleValue(name)
    override def getUDTValue(i: Int): UDTValue = row.get.getUDTValue(i)
    override def getUDTValue(name: String): UDTValue = row.get.getUDTValue(name)
  }
}
