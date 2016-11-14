package com.hadoop.assignment.question1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LocationFindingMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String userId = tokens[0].trim();
        String locationTimeStamp = tokens[1] + "," + tokens[2];

        context.write(new Text(userId), new Text(locationTimeStamp));
    }
}

