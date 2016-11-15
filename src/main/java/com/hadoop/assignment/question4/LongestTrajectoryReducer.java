package com.hadoop.assignment.question4;

import com.hadoop.assignment.utils.ComparatorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by quocnghi on 15/11/16.
 */
public class LongestTrajectoryReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text userKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        SortedMap<String, String> sortedMap = new TreeMap<>(ComparatorUtils.getDescendingYearMonthDayComparator());

        for(Text value : values){
            String[] tokens = value.toString().split(",");
            sortedMap.put(tokens[0], StringUtils.join(Arrays.copyOfRange(tokens,1,tokens.length),","));
        }
        context.write(userKey,new Text(sortedMap.get(sortedMap.firstKey())));
    }
}
