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

import com.datastax.driver.core.{ Cluster, Session, SimpleStatement }
import com.datastax.driver.core.policies.{ LoadBalancingPolicy, Policies, ReconnectionPolicy, RetryPolicy }
import java.util.{ ArrayList, Collections }
import org.apache.hadoop.mapred.JobConf
import scala.collection.SortedSet
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random

object CassandraVNodeTools {

  def getCredentialsOption(conf: JobConf): Option[(String, String)] = {
    Option(CassandraVNodeConfigHelper.getUserName(conf)) match {
      case Some(name) => Option(CassandraVNodeConfigHelper.
          getUserPwd(conf)).map(pwd => (name, pwd)).
          orElse(Some((name, "")))
      case _ => None
    }
  }
  
  def getCluster(hosts: Set[String], port: Int, credentials: Option[(String, String)], 
      loadBalancing: LoadBalancingPolicy = Policies.defaultLoadBalancingPolicy): Cluster = {
    val reconnectPolicy: ReconnectionPolicy = Policies.defaultReconnectionPolicy
    val retryPolicy: RetryPolicy = Policies.defaultRetryPolicy
    val clusterBuilder = Cluster.builder()
    hosts.foreach(host => {
      val hostPort = host.split(":")
      hostPort.length match {
        case 2 => {
          clusterBuilder.addContactPoint(hostPort.apply(0)).withPort(hostPort.apply(1).toInt)
        }
        case _ => clusterBuilder.addContactPoint(host).withPort(port)
      }      
    })
    credentials.map(cred => clusterBuilder.withCredentials(cred._1, cred._2))
    clusterBuilder.withLoadBalancingPolicy(loadBalancing)
    clusterBuilder.withReconnectionPolicy(reconnectPolicy)
    clusterBuilder.withRetryPolicy(retryPolicy)
    clusterBuilder.build() 
  } 

  /**
   * return Splits for the given keyspace in the datacenter 
   */
  def getSplitsForKeyspace(cluster: Cluster, keyspace: String, dc: Option[String]): Array[CassandraVNodeSplit] = {
    val tokenmap = cluster.getMetadata().getTokenMap(keyspace)
    val tokenlist = new ArrayList(tokenmap.keySet())
    Collections.sort(tokenlist)
    val tokenKeyList = tokenlist.asScala
    val splitBuffer = ListBuffer[CassandraVNodeSplit]()
    for(i <- 0 until tokenKeyList.size) {
      val starttoken = tokenKeyList(i)
      val hosts = tokenmap.get(starttoken).asScala.filter(host => dc.map(_ == host.getDatacenter).getOrElse(true) && host.isUp).
        map(_.getAddress().getHostAddress()).toSet
      val randomHost = hosts.toList(new Random().nextInt(hosts.size))
      val endtoken = if(i < (tokenKeyList.size - 1)) {
        splitBuffer += new CassandraVNodeSplit(Some(starttoken.toString()), Some(tokenKeyList(i + 1).toString()), randomHost, hosts)
      } else {
        splitBuffer += new CassandraVNodeSplit(Some(starttoken.toString()), None, randomHost, hosts)
        splitBuffer += new CassandraVNodeSplit(None, Some(tokenKeyList(0).toString()), randomHost, hosts)        
      }
    }
    splitBuffer.toArray
  }
}
