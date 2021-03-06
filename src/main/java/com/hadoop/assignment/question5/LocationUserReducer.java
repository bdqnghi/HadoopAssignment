package com.hadoop.assignment.question5;

import com.hadoop.assignment.utils.ComparatorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by quocnghi on 11/16/16.
 */
public class LocationUserReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text locationUserKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        SortedSet<String> sortedSet = new TreeSet<>(ComparatorUtils.getAscendingTimeStampComparator());

        for (Text value : values) {
            sortedSet.add(new String(value.toString()));
        }

        List<String> list = new ArrayList<>(sortedSet);
        String previous = list.get(0);
        int residencyTime = 0; // in seconds
        int count = 1;
        for (String timestamp : list) {
            int timeDiff = calculateTImeDiff(previous, timestamp);
            if (timeDiff > 70 || count == list.size()) {
                if (residencyTime >= 300) {
                    if (count == list.size()) {
                        residencyTime = residencyTime + timeDiff;
                    }
                    context.write(locationUserKey, new IntWritable(residencyTime));
                }
                residencyTime = 0;
            } else {
                residencyTime = residencyTime + timeDiff;

            }
            previous = timestamp;
            count++;
        }
    }

    private Integer calculateTImeDiff(String t1, String t2) {
        org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-mm-dd HH:mm:ss");
        DateTime dt1 = formatter.parseDateTime(t1);
        DateTime dt2 = formatter.parseDateTime(t2);
        return Seconds.secondsBetween(dt1, dt2).getSeconds();
    }
}
