package com.hadoop.assignment.question5;

import com.hadoop.assignment.question4.LongestTrajectoryMapper;
import com.hadoop.assignment.question4.LongestTrajectoryReducer;
import com.hadoop.assignment.utils.PathUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class QuestionFiveJob extends Configured implements Tool {

    public static String INPUT_PATH = PathUtils.TEMP_PATH;
    public static String TEMP_PATH = PathUtils.TEMP_PATH;
    public static String TEMP2_PATH = PathUtils.TEMP2_PATH;
    public static String OUTPUT_PATH = PathUtils.OUTPUT_PATH;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new QuestionFiveJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String currentDir = System.getProperty("user.dir");
        if (args.length == 0) {
            System.err.println("Please specified input parameters");
        } else {
            INPUT_PATH = currentDir + args[0];
            OUTPUT_PATH = currentDir + args[1];
            TEMP_PATH = currentDir + "temp";
            TEMP2_PATH = currentDir + "temp2";
        }

        //-----------------------------------------------------------
        FileUtils.deleteDirectory(new File(TEMP_PATH));
        conf.set("mapred.textoutputformat.separator", ",");

        Job job1 = new Job(conf, "job1");

        job1.setJarByClass(QuestionFiveJob.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(LocationUserMapper.class);
        job1.setReducerClass(LocationUserReducer.class);

        FileInputFormat.setInputPaths(job1, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(TEMP_PATH));

        boolean success = job1.waitForCompletion(true);

        // -------------------------------------------------

        if (success) {
            FileUtils.deleteDirectory(new File(TEMP2_PATH));

            conf.set("mapred.textoutputformat.separator", ",");
            Job job2 = new Job(conf, "job2");

            job2.setJarByClass(QuestionFiveJob.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setMapperClass(AvgResidencyEachUserMapper.class);
            job2.setReducerClass(AvgResidencyEachUserReducer.class);

            FileInputFormat.setInputPaths(job2, new Path(TEMP_PATH));
            FileOutputFormat.setOutputPath(job2, new Path(TEMP2_PATH));

            boolean success2 =  job2.waitForCompletion(true);
            if (success2) {
                FileUtils.deleteDirectory(new File(OUTPUT_PATH));

                conf.set("mapred.textoutputformat.separator", " ");
                Job job3 = new Job(conf, "job3");

                job3.setJarByClass(QuestionFiveJob.class);

                job3.setMapOutputKeyClass(Text.class);
                job3.setMapOutputValueClass(Text.class);

                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(DoubleWritable.class);

                job3.setInputFormatClass(TextInputFormat.class);
                job3.setOutputFormatClass(TextOutputFormat.class);

                job3.setMapperClass(AvgResidencyMapper.class);
                job3.setReducerClass(AvgResidencyReducer.class);

                FileInputFormat.setInputPaths(job3, new Path(TEMP2_PATH));
                FileOutputFormat.setOutputPath(job3, new Path(OUTPUT_PATH));

                job3.waitForCompletion(true);
            }
        }

        return 0;
    }
}
