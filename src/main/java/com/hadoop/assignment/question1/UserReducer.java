package com.hadoop.assignment.question1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by quocnghi on 11/10/16.
 */
public class UserReducer extends Reducer<UserLocationKey, Text, Text, Text> {

    private static final Log _log = LogFactory.getLog(UserReducer.class);

    @Override
    public void reduce(UserLocationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text k = new Text(key.toString());
        int count = 0;

        Iterator<Text> it = values.iterator();
        while(it.hasNext()) {
            Text v = new Text(it.next().toString());
            context.write(k, v);
            _log.debug(k.toString() + " => " + v.toString());
            count++;
        }

        _log.debug("count = " + count);
    }
}
