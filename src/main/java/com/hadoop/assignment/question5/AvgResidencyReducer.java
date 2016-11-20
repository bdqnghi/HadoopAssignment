package com.hadoop.assignment.question5;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by quocnghi on 11/16/16.
 */
public class AvgResidencyReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    public void reduce(Text locationKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

        for (Text value : values) {
            descriptiveStatistics.addValue(new Double(value.toString().split(",")[1]));
        }

        Double minutes = Double.valueOf(Math.round(descriptiveStatistics.getMean() / 60));
        context.write(locationKey, new DoubleWritable(minutes));
    }
}
