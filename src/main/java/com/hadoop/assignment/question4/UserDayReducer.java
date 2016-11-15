package com.hadoop.assignment.question4;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by quocnghi on 15/11/16.
 */
public class UserDayReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text userDayKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<>();

        for (Text value : values) {
            set.add(new String(value.toString()));
        }

        String join = StringUtils.join(set, ",");
        String userId = userDayKey.toString().split(",")[0];

        context.write(userDayKey, new Text(join + "," + set.size()));
    }

}
