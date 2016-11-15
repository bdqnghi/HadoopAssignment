package com.hadoop.assignment.question4;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by quocnghi on 15/11/16.
 */
public class LongestTrajectoriesMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String userId = tokens[0];
        String[] remains = Arrays.copyOfRange(tokens, 1, tokens.length);
        context.write(new Text(userId), new Text(StringUtils.join(remains, ",")));
    }
}
