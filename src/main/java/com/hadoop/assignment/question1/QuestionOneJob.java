package com.hadoop.assignment.question1;


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
public class QuestionOneJob extends Configured implements Tool {

    public static String INPUT_PATH = PathUtils.TEMP_PATH;
    public static String TEMP_PATH = PathUtils.TEMP_PATH;
    public static String TEMP2_PATH = PathUtils.TEMP2_PATH;
    public static String OUTPUT_PATH = PathUtils.OUTPUT_PATH;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new QuestionOneJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String currentDir = System.getProperty("user.dir");
        System.out.println("Current dir : " + currentDir);
        if (args.length == 0) {
            System.err.println("Please specified input parameters");
        } else {
            INPUT_PATH = args[0];
            OUTPUT_PATH = args[1];
            TEMP_PATH = "temp";
            TEMP2_PATH = "temp2";
        }
        //-----------------------------------------------------------
        FileUtils.deleteDirectory(new File(TEMP_PATH));
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

        FileInputFormat.setInputPaths(timePeriodJob, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(timePeriodJob, new Path(TEMP_PATH));

        boolean success = timePeriodJob.waitForCompletion(true);

        // -------------------------------------------------

        if (success) {
            FileUtils.deleteDirectory(new File(OUTPUT_PATH));
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

            FileInputFormat.setInputPaths(locationFindingJob, new Path(TEMP_PATH));
            FileOutputFormat.setOutputPath(locationFindingJob, new Path(OUTPUT_PATH));

            locationFindingJob.waitForCompletion(true);
        }

        return 0;
    }
}
