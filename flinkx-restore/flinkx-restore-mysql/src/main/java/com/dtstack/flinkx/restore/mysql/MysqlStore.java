package com.dtstack.flinkx.restore.mysql;

import com.dtstack.flinkx.cdc.CdcConf;
import com.dtstack.flinkx.cdc.store.Store;

import org.apache.flink.table.data.RowData;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/6 星期一
 */
public class MysqlStore extends Store {

    public MysqlStore(CdcConf conf) {
        // init store
    }

    @Override
    public void store(RowData data) {}

    @Override
    public void open() {}

    @Override
    public void closeSubclass() {}
}
