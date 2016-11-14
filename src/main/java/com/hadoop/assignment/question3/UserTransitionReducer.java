package com.hadoop.assignment.question3;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by quocnghi on 11/15/16.
 */
public class UserTransitionReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text userKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        for (IntWritable value : values) {
            descriptiveStatistics.addValue(value.get());
        }
        context.write(userKey, new DoubleWritable(descriptiveStatistics.getMean()));
    }
}
