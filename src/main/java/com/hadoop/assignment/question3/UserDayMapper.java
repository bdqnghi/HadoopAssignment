package com.hadoop.assignment.question3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quocnghi on 11/15/16.
 */
public class UserDayMapper extends Mapper<LongWritable, Text, Text, Text> {
    // Sample hour : 2016-03-12 21:40:59
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String userId = tokens[1];
        String locationId = tokens[2];
        String timestamp = tokens[0];
        String[] times = timestamp.split(" ");
        String day = times[0];
        String hourMinutesSec[] = times[1].split(":");
        context.write(new Text(userId + "," + day + "," + hourMinutesSec[0]), new Text(locationId + "," + hourMinutesSec[1] + ":" + hourMinutesSec[2]));
    }
}
