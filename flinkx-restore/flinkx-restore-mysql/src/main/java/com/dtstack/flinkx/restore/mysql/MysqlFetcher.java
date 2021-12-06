package com.dtstack.flinkx.restore.mysql;

import com.dtstack.flinkx.cdc.CdcConf;
import com.dtstack.flinkx.cdc.store.Fetcher;
import org.apache.flink.table.data.RowData;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/6 星期一
 */
public class MysqlFetcher extends Fetcher {

    public MysqlFetcher(CdcConf conf) {
        // init fetcher
    }

    @Override
    public boolean fetch(RowData data) {
        return false;
    }

    @Override
    public void open() {}

    @Override
    public void closeSubclass() {}
}
