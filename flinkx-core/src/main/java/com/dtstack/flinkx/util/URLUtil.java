package com.dtstack.flinkx.util;

import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.Callable;

/**
 * @author jiangbo
 * @date 2018/7/10 14:08
 */
public class URLUtil {

    private static int MAX_RETRY_TIMES = 3;

    private static int SLEEP_TIME_MILLI_SECOND = 2000;

    public static InputStream open(String url) throws Exception{
        return RetryUtil.executeWithRetry(new Callable<InputStream>() {
            @Override
            public InputStream call() throws Exception{
                return new URL(url).openStream();
            }
        },MAX_RETRY_TIMES,SLEEP_TIME_MILLI_SECOND,false);
    }
}
