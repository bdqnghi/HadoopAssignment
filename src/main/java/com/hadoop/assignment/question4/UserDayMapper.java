package com.hadoop.assignment.question4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by quocnghi on 15/11/16.
 */
public class UserDayMapper extends Mapper<LongWritable, Text, Text, ArrayWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String previous = configuration.get("previous.location","");

        String[] tokens = value.toString().split(",");
        String userId = tokens[1];
        String locationId = tokens[2];
        String timestamp = tokens[0];
        String[] times = timestamp.split(" ");
        String day = times[0];
        List<String> locations = new ArrayList<>();
        if(!locationId.equals(previous)){
            configuration.set("previous.location",locationId);
            locations.add(locationId);
            String[] locationsArray = locations.toArray(new String[locations.size()]);
            context.write(new Text(userId + "," + day), new ArrayWritable(locationsArray));
        }

    }
}
