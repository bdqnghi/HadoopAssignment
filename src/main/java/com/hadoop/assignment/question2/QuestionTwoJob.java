package com.hadoop.assignment.question2;


import com.hadoop.assignment.utils.PathUtils;
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
public class QuestionTwoJob extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new QuestionTwoJob(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        //-----------------------------------------------------------
        FileUtils.deleteDirectory(new File(PathUtils.TEMP_PATH));
        conf.set("mapred.textoutputformat.separator", ",");
        Job job1 = new Job(conf, "job1");

        job1.setJarByClass(QuestionTwoJob.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(LocationUserCountMapper.class);
        job1.setReducerClass(LocationUserCountReducer.class);

        FileInputFormat.setInputPaths(job1, new Path(PathUtils.INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(PathUtils.TEMP_PATH));

        boolean success = job1.waitForCompletion(true);

        // -------------------------------------------------

        if (success) {
            FileUtils.deleteDirectory(new File(PathUtils.OUTPUT_PATH));

            Job job2 = new Job(conf, "job2");

            job2.setJarByClass(QuestionTwoJob.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setMapperClass(LocationCountMapper.class);
            job2.setReducerClass(LocationCountReducer.class);

            FileInputFormat.setInputPaths(job2, new Path(PathUtils.TEMP_PATH));
            FileOutputFormat.setOutputPath(job2, new Path(PathUtils.OUTPUT_PATH));

            job2.waitForCompletion(true);
        }

        return 0;
    }
}
