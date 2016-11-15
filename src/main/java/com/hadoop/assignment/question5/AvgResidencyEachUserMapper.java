package com.hadoop.assignment.question5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quocnghi on 11/16/16.
 */
public class AvgResidencyEachUserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String userId = tokens[1];
        String locationId = tokens[0];
        Integer residencyTime = Integer.parseInt(tokens[2]);
        context.write(new Text(locationId + "," + userId), new IntWritable(residencyTime));
    }
}
