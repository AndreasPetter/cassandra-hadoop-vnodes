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
import scala.collection.JavaConverters._

/**
 * a very simple converter between mapred and mapreduce API configurations,
 * which converts CassandraVNodeConfigHelper parameters.
 */
object CassandraVNodeConfigurationConverter {

  def apply(conf: Configuration): JobConf = {
    val result = new JobConf
    CassandraVNodeConfigHelper.setClusterName(result, CassandraVNodeConfigHelper.getClusterName(conf))
    CassandraVNodeConfigHelper.setDatacenter(result, CassandraVNodeConfigHelper.getDatacenter(conf))
    CassandraVNodeConfigHelper.setColumnFamily(result, CassandraVNodeConfigHelper.getColumnFamily(conf))
    CassandraVNodeConfigHelper.setColumns(result, CassandraVNodeConfigHelper.getColumns(conf))
    CassandraVNodeConfigHelper.setKeyspace(result, CassandraVNodeConfigHelper.getKeyspace(conf))
    CassandraVNodeConfigHelper.setNodes(result, CassandraVNodeConfigHelper.getNodes(conf).asJava)
    CassandraVNodeConfigHelper.setNativePort(result, CassandraVNodeConfigHelper.getNativePort(conf))
    CassandraVNodeConfigHelper.setWhereClauses(result, CassandraVNodeConfigHelper.getWhereClauses(conf))
    CassandraVNodeConfigHelper.setUserName(result, CassandraVNodeConfigHelper.getUserName(conf))
    CassandraVNodeConfigHelper.setUserPwd(result, CassandraVNodeConfigHelper.getUserPwd(conf))
    result
  }
}
