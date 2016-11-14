package com.hadoop.assignment.question1;

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

        Comparator<Integer> reverseComparator = new Comparator<Integer>() {
            @Override
            public int compare(Integer i1, Integer i2) {
                return i2.compareTo(i1);
            }
        };

        SortedMap<Integer, String> map = new TreeMap<>(reverseComparator);
        for (Text t : values) {
            String value = t.toString();
            String[] tokens = value.split(",");
            String location = tokens[0];
            String timePeriod = tokens[1];
            map.put(Integer.parseInt(timePeriod), location);
        }

        List<String> locations = new ArrayList<>(map.values());
        String topNLocations = getTopNLocation(10, locations);
        context.write(key, new Text(topNLocations));
    }

    private String getTopNLocation(int N, List<String> locations) {
        StringBuilder sb = new StringBuilder();
        int realN = N;
        if (locations.size() < N) {
            realN = locations.size();
        }
        for (int i = 0; i < realN; i++) {
            sb.append(locations.get(i)).append(",");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
