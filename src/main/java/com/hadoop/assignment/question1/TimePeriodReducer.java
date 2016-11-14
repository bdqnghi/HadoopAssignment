package com.hadoop.assignment.question1;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by quocnghi on 11/12/16.
 */
public class TimePeriodReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text k = new Text(key.toString());

        List<Text> temp = Lists.newArrayList(values);

        temp = sort(temp);
        if (temp.size() == 1) {
            context.write(k, new IntWritable(0));
        } else {

            String previous = temp.get(0).toString();
            int sum = 0;
            for (Text t : temp) {
                String current = t.toString();
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
            context.write(k, new IntWritable(sum));
        }
    }

    private List<Text> sort(List<Text> input) {
        Collections.sort(input, new Comparator<Text>() {
            public int compare(Text t1, Text t2) {
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
