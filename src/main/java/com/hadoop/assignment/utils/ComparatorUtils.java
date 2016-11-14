package com.hadoop.assignment.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Comparator;

/**
 * Created by quocnghi on 11/15/16.
 */
public class ComparatorUtils {

    public static Comparator getMinuteSecondComparator(){
        return new Comparator<String>() {
            public int compare(String t1, String t2) {
                String dateTimeString = t1.toString();
                org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("mm:ss");
                DateTime dt1 = formatter.parseDateTime(dateTimeString);

                dateTimeString = t2.toString();
                DateTime dt2 = formatter.parseDateTime(dateTimeString);

                if (dt1.isBefore(dt2))
                    return -1;
                else
                    return 1;
            }
        };
    }
}
