package com.hadoop.secondary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by quocnghi on 11/10/16.
 */
public class StockApp {
    public static void main( String[] args ) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SsJob(), args);
        System.exit(res);
    }
}
