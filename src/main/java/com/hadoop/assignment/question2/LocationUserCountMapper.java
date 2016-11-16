package com.hadoop.assignment.question2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LocationUserCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(!value.toString().equals("time,id,location")){
            String[] tokens = value.toString().split(",");
            String userId = tokens[1].trim();
            String locationId = tokens[2].trim();
            context.write(new Text(locationId + "," + userId), new IntWritable(1));
        }
    }
}
