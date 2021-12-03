package com.dtstack.flinkx.cdc.store;

import org.apache.flink.table.data.RowData;

import java.util.concurrent.TimeUnit;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/2 星期四
 */
public class MySQLFetcher extends Fetcher {

    @Override
    public boolean fetch(RowData data) {
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException ignore) {
        }
        System.out.println("Data 已经被处理");
        return true;
    }

    @Override
    public void open() {
        System.out.println("MySQL Fetcher open()");
    }

    @Override
    public void close() {
        System.out.println("MySQL Fetcher close()");
    }
}
