package com.hadoop.assignment.question5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quocnghi on 11/16/16.
 */
public class LocationUserMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(!value.toString().equals("time,id,location")){
            String[] tokens = value.toString().split(",");
            String userId = tokens[1].trim();
            String locationId = tokens[2].trim();
            String timestamp = tokens[0];
            context.write(new Text(locationId + "," + userId), new Text(timestamp));
        }
    }
}
