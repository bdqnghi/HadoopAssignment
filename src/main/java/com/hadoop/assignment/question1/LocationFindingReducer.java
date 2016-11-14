package com.hadoop.assignment.question1;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by quocnghi on 11/12/16.
 */
public class LocationFindingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Text> list = Lists.newArrayList(values);
        Map<Integer, String> map = new TreeMap<>();

        for (Text t : list) {
            String value = t.toString();
            String[] tokens = value.split(",");
            String location = tokens[0];
            String timePeriod = tokens[1];
            map.put(Integer.parseInt(timePeriod), location);
        }

        List<String> locations = new ArrayList<>(map.values());
        for (int i = 0; i < 10; i++) {
            context.write(key, new Text(locations.get(i)));
        }
    }
}
