package com.dtstack.flinkx.cdc.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/8 星期三
 */
public class LogExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error(
                String.format(
                        "An error occurred during the sending data. thread name : [%s]",
                        t.getName()),
                e);
    }
}
