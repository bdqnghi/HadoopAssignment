package com.hadoop.assignment.question4;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Ordering;
import com.hadoop.assignment.utils.ComparatorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by quocnghi on 15/11/16.
 */
public class LongestTrajectoriesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text userKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Multimap<Integer,String> multimap = MultimapBuilder.treeKeys(ComparatorUtils.getDescendingIntegerComparator()).arrayListValues().build();
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            String locations = StringUtils.join(Arrays.copyOfRange(tokens, 0, tokens.length - 1), ",");
            multimap.put(Integer.parseInt(tokens[tokens.length - 1]), locations);
        }

        List<Integer> listKeys = new ArrayList<>(multimap.keySet());
        Integer previousLongestTrajectory = listKeys.get(0);

        int count = 0;
        for (Map.Entry<Integer, String> entry : multimap.entries()) {
            if (entry.getKey().compareTo(previousLongestTrajectory) == 0) {
                List<String> locationValues = new ArrayList<>(multimap.get(entry.getKey()));
                context.write(userKey, new Text(locationValues.get(count)));
            }
            count++;
        }
    }


}
