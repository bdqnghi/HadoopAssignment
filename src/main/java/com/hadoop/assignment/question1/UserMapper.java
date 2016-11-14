package com.hadoop.assignment.question1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quocnghi on 11/10/16.
 */
public class UserMapper extends Mapper<LongWritable, Text, UserLocationKey, Text> {

    private static final Log _log = LogFactory.getLog(UserMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        String userId = tokens[1].trim();
        String locationId = tokens[2].trim();
        Text timestamp = new Text(tokens[0].trim());

        UserLocationKey userKey = new UserLocationKey(userId, locationId);
        //DoubleWritable stockValue = new DoubleWritable(v);

        context.write(userKey, timestamp);
    }
}
