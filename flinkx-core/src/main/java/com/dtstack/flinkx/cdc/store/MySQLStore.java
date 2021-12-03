package com.dtstack.flinkx.cdc.store;

import org.apache.flink.table.data.RowData;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/2 星期四
 */
public class MySQLStore extends Store {

    @Override
    public void store(RowData data) {
        System.out.println("存储data到外部存储");
    }

    @Override
    public void open() {
        System.out.println("MySQL Store open()");
    }

    @Override
    public void closeSubclass() {
        System.out.println("MySQL Store closed.");
    }
}
