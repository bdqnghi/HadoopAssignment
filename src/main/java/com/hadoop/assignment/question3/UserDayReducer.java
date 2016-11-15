package com.hadoop.assignment.question3;

import com.hadoop.assignment.utils.ComparatorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by quocnghi on 11/15/16.
 */
public class UserDayReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text userDayHourKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        SortedMap<String, String> sortedMap = new TreeMap<>(ComparatorUtils.getDescendMinuteSecondComparator());

        for (Text value : values) {
            String s = value.toString();
            String tokens[] = s.split(",");
            sortedMap.put(tokens[1], tokens[0]);
        }

        List<String> locations = new ArrayList<>(sortedMap.values());
        String previous = locations.get(0);
        int transitions = 0;
        for (String location : locations) {
            String current = location;
            if (!previous.equals(current)) {
                transitions++;
            }
        }

        String tokens[] = userDayHourKey.toString().split(",");
        context.write(new Text(tokens[0]), new IntWritable(transitions));
    }
}
