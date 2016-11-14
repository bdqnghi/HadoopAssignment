package com.hadoop.assignment.question1;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
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
        List<String> temp =  new ArrayList<>();

        for(Text value : values){
            temp.add(new String(value.toString()));
        }

        temp = sort(temp);
        if (temp.size() == 1) {
            context.write(userLocationKey, new IntWritable(0));
        } else {

            String previous = temp.get(0).toString();
            int sum = 0;
            for (String current : temp) {
                if(previous.compareTo(current) == 0){
                    continue;
                }
                else{
                    String dateTimeString = previous;
                    org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("MM/dd/yy HH:mm");
                    DateTime dt1 = formatter.parseDateTime(dateTimeString);

                    dateTimeString = current.toString();
                    DateTime dt2 = formatter.parseDateTime(dateTimeString);

                    if (dt1.isBefore(dt2)) {
                        Seconds seconds = Seconds.secondsBetween(dt1, dt2);
                        sum = sum + seconds.getSeconds();

                    }
                    previous = current;
                }

            }
            context.write(userLocationKey, new IntWritable(sum));
        }
    }

    private List<String> sort(List<String> input) {
        Collections.sort(input, new Comparator<String>() {
            public int compare(String t1, String t2) {
                String dateTimeString = t1.toString();
                org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("MM/dd/yy HH:mm");
                DateTime dt1 = formatter.parseDateTime(dateTimeString);

                dateTimeString = t2.toString();
                DateTime dt2 = formatter.parseDateTime(dateTimeString);

                if (dt1.isBefore(dt2))
                    return -1;
                else
                    return 1;
            }
        });
        return input;
    }
}
