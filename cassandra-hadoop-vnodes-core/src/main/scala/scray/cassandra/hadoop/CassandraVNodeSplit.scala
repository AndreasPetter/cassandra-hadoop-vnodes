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

import java.io.{ DataInput, DataOutput }
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapreduce.{ InputSplit => NewInputSplit }
import scala.collection.mutable.ArrayBuffer

/**
 * Hadoop mapred InputSplit containing the Cassandra query parameters for
 * the token range queries issued on the mappers.
 * Need to make that vars in order to easily implement InputSplit.
 */
@serializable class CassandraVNodeSplit() extends NewInputSplit with InputSplit with Writable {
  
  def this(startToken: Option[String], endToken: Option[String], host: String, allhosts: Set[String]) = {
    this()
    this.startToken = startToken
    this.endToken = endToken
    this.host = host
    this.allhosts = allhosts
  }

  var startToken: Option[String] = None
  var endToken: Option[String] = None
  var host: String = ""
  var allhosts: Set[String] = Set[String]()
  
  /**
   * A Location can be provided if this is a Cassandra host.
   * Use only the host where the split it is likely to be used.
   */
  override def getLocations(): Array[String] = Array(host)
    
  override def readFields(in: DataInput): Unit = {
    if(in.readBoolean()) {
      startToken = Some(in.readUTF())
    } else {
      startToken = None
    }
    if(in.readBoolean()) {
      endToken = Some(in.readUTF())
    } else {
      endToken = None
    }
    host = in.readUTF()
    val number = in.readInt()
    val hostBuffer = new ArrayBuffer[String]
    for(i <- 0 until number) {
      hostBuffer += in.readUTF
    }
    allhosts = hostBuffer.toSet
  }
    
  override def write(out: DataOutput): Unit = {
    out.writeBoolean(startToken.isDefined)
    startToken.map(out.writeUTF(_))
    out.writeBoolean(endToken.isDefined)
    endToken.map(out.writeUTF(_))
    out.writeUTF(host)
    out.writeInt(allhosts.size)
    for(ahost <- allhosts) {
      out.writeUTF(ahost)
    }
  }
    
  def getEndToken: Option[String] = endToken
  def getStartToken: Option[String] = startToken
  def getDefaultHost: String = host
  def getAllHosts: Set[String] = allhosts
  override def getLength(): Long = 0L // we would need an expensive count query for that...
  
  override def toString: String = s"Split: >= $startToken AND < $endToken ON $host OR OPTIONALLY ON $allhosts"
}
  
object CassandraVNodeSplit {
  def apply(x: Unit): CassandraVNodeSplit = new CassandraVNodeSplit
  def apply(startToken: Option[String], endToken: Option[String], host: String, allhosts: Set[String]): CassandraVNodeSplit =
    new CassandraVNodeSplit(startToken, endToken, host, allhosts)
}
