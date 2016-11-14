package com.hadoop.assignment.question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quocnghi on 11/15/16.
 */
public class UserTransitionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        Text userId = new Text(tokens[0]);
        IntWritable transitions = new IntWritable(Integer.parseInt(tokens[1]));
        context.write(new Text(userId), transitions);
    }
}
