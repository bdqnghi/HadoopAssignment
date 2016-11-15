package com.hadoop.assignment.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Comparator;

/**
 * Created by quocnghi on 11/15/16.
 */
public class ComparatorUtils {

    public static Comparator getDescendMinuteSecondComparator(){
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

    public static Comparator getDescendingIntegerComparator(){
        return new Comparator<Integer>() {
            @Override
            public int compare(Integer i1, Integer i2) {
                return i2.compareTo(i1);
            }
        };
    }

    public static Comparator getDescendingTimeStampComparator(){
        return new Comparator<String>() {
            @Override
            public int compare(String t1, String t2) {
                String dateTimeString = t1.toString();
                org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-mm-dd");
                DateTime dt1 = formatter.parseDateTime(dateTimeString);

                dateTimeString = t2.toString();
                DateTime dt2 = formatter.parseDateTime(dateTimeString);

                if (dt1.isAfter(dt2))
                    return -1;
                else
                    if(dt1.isBefore(dt2))
                        return 1;
                    else
                        return 0;
            }
        };
    }
}
