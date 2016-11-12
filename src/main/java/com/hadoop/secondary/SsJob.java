/**
 * Copyright 2012 Jee Vang 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *  Unless required by applicable law or agreed to in writing, software 
 *  distributed under the License is distributed on an "AS IS" BASIS, 
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 *  See the License for the specific language governing permissions and 
 *  limitations under the License. 
 */
package com.hadoop.secondary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Secondary sort job.
 * @author Jee Vang
 *
 */
public class SsJob extends Configured implements Tool {

	/**
	 * Main method. You should specify -Dmapred.input.dir and -Dmapred.output.dir.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SsJob(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("mapred.textoutputformat.separator", ",");

		Job job = new Job(conf, "secondary sort");

		job.setJarByClass(SsJob.class);
//		job.setPartitionerClass(NaturalKeyPartitioner.class);
//		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		job.setMapOutputKeyClass(StockKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(SsMapper.class);
		job.setReducerClass(SsReducer.class);

		FileSystem fs = FileSystem.get(new Configuration());
// true stands for recursively deleting the folder you gave
		fs.delete(new Path("/Users/quocnghi/hadoop/stock-output"), true);

		FileInputFormat.setInputPaths(job, new Path("/Users/quocnghi/hadoop/location-input"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/quocnghi/hadoop/stock-output"));

		job.waitForCompletion(true);
		
		return 0;
	}

}
