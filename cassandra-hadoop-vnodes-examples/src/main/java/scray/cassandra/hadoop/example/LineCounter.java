/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package scray.cassandra.hadoop.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scray.cassandra.hadoop.CassandraVNodeConfigHelper;
import scray.cassandra.hadoop.CassandraVNodeInputFormat;

import com.datastax.driver.core.Row;

/**
 * Implements a very simple map-reduce job that counts the number of Rows in a ColumnFamily 
 */
public class LineCounter extends Configured implements Tool
{
    private static final Logger logger = LoggerFactory.getLogger(LineCounter.class);
    
    // TODO: change this
    static final String KEYSPACE = "mykeyspace";
    
    // TODO: change this
    static final String CASS_HOST = "localhost";
    
    // TODO: change this
    static final String COLUMN_FAMILY = "mycolumnfamily";

    // TODO: change this
    static final String DATA_CENTER = "DC1";
    
    // TODO: change this
    static final String PARTITION_KEY_COLUMN_NAME = "key";
    
    private static final String OUTPUT_PATH_DIR = "/tmp/line_counter";

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new Configuration(), new LineCounter(), args);
        System.exit(0);
    }

    public static class RowMapper implements Mapper<Long, Row, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

		@Override
		public void configure(JobConf arg0) {}

		@Override
		public void close() throws IOException {}

		@Override
		public void map(Long key, Row row,
				OutputCollector<Text, IntWritable> collector, Reporter arg3)
				throws IOException {
			String value = row.getString(PARTITION_KEY_COLUMN_NAME);
			word.set(value);
			logger.debug("Found Row with key {1}", value);
            collector.collect(word, one);
		}
    }

    public static class ReducerToHDFS implements Reducer<Text, IntWritable, Text, IntWritable>
    {
		@Override
		public void configure(JobConf arg0) {}

		@Override
		public void close() throws IOException {
			if(collector != null) {
				Text key = new Text("Sum");
				IntWritable wrt = new IntWritable(count);
				collector.collect(key, wrt);
			}
		}

		private int count = 0;
		
		private OutputCollector<Text, IntWritable> collector = null;
		
		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> collector, Reporter arg3)
				throws IOException {
            count += 1;
            logger.debug("Counted: {1}", count);
            this.collector = collector;
		}
    }


    public int run(String[] args) throws Exception
    {

        JobConf job = new JobConf(LineCounter.class);
        job.setJobName("Counting number of rows with CassandraVNodes InputFormat");
        job.setJarByClass(LineCounter.class);

        job.setReducerClass(ReducerToHDFS.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_DIR));

        job.setMapperClass(RowMapper.class);
        
        HashSet<String> hosts = new HashSet<String>();
        hosts.add(CASS_HOST);
        CassandraVNodeConfigHelper.setClusterName(job, "Test Cluster");
        CassandraVNodeConfigHelper.setDatacenter(job, DATA_CENTER);
        CassandraVNodeConfigHelper.setKeyspace(job, KEYSPACE);
        CassandraVNodeConfigHelper.setColumnFamily(job, COLUMN_FAMILY);
        CassandraVNodeConfigHelper.setNodes(job, hosts);
        
        job.setInputFormat((Class<InputFormat<Long, Row>>)(Object)CassandraVNodeInputFormat.class);
        
        JobClient.runJob(job);
        
        return 0;
    }
}
