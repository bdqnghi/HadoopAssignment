package com.hadoop.assignment.question5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quocnghi on 11/16/16.
 */
public class AvgResidencyMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String userId = tokens[1];
        String locationId = tokens[0];
        String residencyTime = tokens[2];
        context.write(new Text(locationId), new Text(userId + "," + residencyTime));
    }
}
