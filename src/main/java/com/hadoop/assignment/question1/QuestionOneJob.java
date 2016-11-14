package com.hadoop.assignment.question1;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * Created by quocnghi on 11/10/16.
 */
public class QuestionOneJob extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new QuestionOneJob(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

//        FileUtils.deleteDirectory(new File("/Users/quocnghi/hadoop/location-output"));
//        conf.set("mapred.textoutputformat.separator", ";");
//
//        Job userLocationJob = new Job(conf, "user location group job");
//
//        userLocationJob.setJarByClass(UserGroupJob.class);
////		job.setPartitionerClass(NaturalKeyPartitioner.class);
////		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
//        userLocationJob.setSortComparatorClass(CompositeKeyComparator.class);
//
//        userLocationJob.setMapOutputKeyClass(UserLocationKey.class);
//        userLocationJob.setMapOutputValueClass(Text.class);
//
//        userLocationJob.setOutputKeyClass(Text.class);
//        userLocationJob.setOutputValueClass(Text.class);
//
//        userLocationJob.setInputFormatClass(TextInputFormat.class);
//        userLocationJob.setOutputFormatClass(TextOutputFormat.class);
//
//        userLocationJob.setMapperClass(UserMapper.class);
//        userLocationJob.setReducerClass(UserReducer.class);
//
//        FileInputFormat.setInputPaths(userLocationJob, new Path("/Users/quocnghi/hadoop/location-input"));
//        FileOutputFormat.setOutputPath(userLocationJob, new Path("/Users/quocnghi/hadoop/location-output"));
//
//        userLocationJob.waitForCompletion(true);

        //-----------------------------------------------------------
        FileUtils.deleteDirectory(new File("/Users/quocnghi/hadoop/location-temp"));
        conf.set("mapred.textoutputformat.separator", ",");
        Job timePeriodJob = new Job(conf, "time period job");

        timePeriodJob.setJarByClass(QuestionOneJob.class);

        timePeriodJob.setMapOutputKeyClass(Text.class);
        timePeriodJob.setMapOutputValueClass(Text.class);

        timePeriodJob.setOutputKeyClass(Text.class);
        timePeriodJob.setOutputValueClass(IntWritable.class);

        timePeriodJob.setInputFormatClass(TextInputFormat.class);
        timePeriodJob.setOutputFormatClass(TextOutputFormat.class);

        timePeriodJob.setMapperClass(TimePeriodMapper.class);
        timePeriodJob.setReducerClass(TimePeriodReducer.class);

        FileInputFormat.setInputPaths(timePeriodJob, new Path("/Users/quocnghi/hadoop/location-input"));
        FileOutputFormat.setOutputPath(timePeriodJob, new Path("/Users/quocnghi/hadoop/location-temp"));

        boolean success = timePeriodJob.waitForCompletion(true);

        // -------------------------------------------------

        if (success) {
            FileUtils.deleteDirectory(new File("/Users/quocnghi/hadoop/location-output"));
            conf.set("mapred.textoutputformat.separator", " ");
            Job locationFindingJob = new Job(conf, "time period job");

            locationFindingJob.setJarByClass(QuestionOneJob.class);

            locationFindingJob.setMapOutputKeyClass(Text.class);
            locationFindingJob.setMapOutputValueClass(Text.class);

            locationFindingJob.setOutputKeyClass(Text.class);
            locationFindingJob.setOutputValueClass(Text.class);

            locationFindingJob.setInputFormatClass(TextInputFormat.class);
            locationFindingJob.setOutputFormatClass(TextOutputFormat.class);

            locationFindingJob.setMapperClass(LocationFindingMapper.class);
            locationFindingJob.setReducerClass(LocationFindingReducer.class);

            FileInputFormat.setInputPaths(locationFindingJob, new Path("/Users/quocnghi/hadoop/location-temp"));
            FileOutputFormat.setOutputPath(locationFindingJob, new Path("/Users/quocnghi/hadoop/location-output"));

            locationFindingJob.waitForCompletion(true);
        }

        return 0;
    }
}
