package com.hadoop.assignment.question4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quocnghi on 15/11/16.
 */
public class UserDayMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(!value.toString().equals("time,id,location")){
            String[] tokens = value.toString().split(",");
            String userId = tokens[1];
            String locationId = tokens[2];
            String timestamp = tokens[0];
            String[] times = timestamp.split(" ");
            String day = times[0];
            context.write(new Text(userId + "," + day), new Text(locationId));
        }
    }
}
