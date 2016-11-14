package com.hadoop.assignment.question1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quocnghi on 11/12/16.
 */
public class TimePeriodMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(";");
        String userLocation = tokens[0];
        String timestamp = tokens[1];

        context.write(new Text(userLocation), new Text(timestamp));
    }
}
