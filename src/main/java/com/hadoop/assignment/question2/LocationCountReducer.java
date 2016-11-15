package com.hadoop.assignment.question2;

import com.hadoop.assignment.utils.ComparatorUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by quocnghi on 11/14/16.
 */
public class LocationCountReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text locationKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        SortedMap<Integer, String> map = new TreeMap<>(ComparatorUtils.getDescendingIntegerComparator());
        for (Text value : values) {
            String text = value.toString();
            String[] tokens = text.split(",");
            map.put(Integer.parseInt(tokens[1]),tokens[0]);
        }

        List<String> users = new ArrayList<>(map.values());
        // Get Top N users, in this case, N = 10
        String topNUsers = getTopNUsers(10, users);
        context.write(locationKey, new Text(topNUsers));

    }

    private String getTopNUsers(int N, List<String> locations) {
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
