package com.hadoop.assignment.question1;

import com.hadoop.assignment.utils.ComparatorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by quocnghi on 11/12/16.
 */
public class TimePeriodReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text userLocationKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        SortedSet<String> sortedSet = new TreeSet<>(ComparatorUtils.getDescendingTimeStampComparator());

        for (Text value : values) {
            sortedSet.add(new String(value.toString()));
        }

        List<String> temp = new ArrayList<>(sortedSet);

        String previous = temp.get(0).toString();
        int sum = 0; // in seconds
        int spentTimes = 0;
        int count = 1;
        for (String timestamp : temp) {
            int timeDiff = calculateTImeDiff(previous, timestamp);
            if (timeDiff > 70 || count == temp.size()) {
                spentTimes = spentTimes + sum;
                if (count == temp.size()) {
                    spentTimes = spentTimes + timeDiff;
                }
                sum = 0;
            } else {
                sum = sum + timeDiff;
            }
            previous = timestamp;
            count++;
        }
        context.write(userLocationKey, new IntWritable(spentTimes));
    }

    private Integer calculateTImeDiff(String t1, String t2) {
        org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-mm-dd HH:mm:ss");
        DateTime dt1 = formatter.parseDateTime(t1);
        DateTime dt2 = formatter.parseDateTime(t2);
        return Seconds.secondsBetween(dt1, dt2).getSeconds();
    }
}
