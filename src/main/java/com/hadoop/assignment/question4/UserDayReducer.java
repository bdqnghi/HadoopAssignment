package com.hadoop.assignment.question4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by quocnghi on 15/11/16.
 */
public class UserDayReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text userKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(Text value : values){
            sum = sum + value.get();
        }

    }
}
