package com.dtstack.chunjun.connector.starrocks.sink;

import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.streamload.StreamLoadManager;

import org.apache.flink.table.data.RowData;

import java.util.List;

/** @author liuliu 2022/7/28 */
public abstract class StarRocksWriteProcessor {

    protected final StreamLoadManager streamLoadManager;
    protected final StarRocksConf starRocksConf;

    public StarRocksWriteProcessor(
            StreamLoadManager streamLoadManager, StarRocksConf starRocksConf) {
        this.streamLoadManager = streamLoadManager;
        this.starRocksConf = starRocksConf;
    }

    public abstract void write(List<RowData> rowDataList) throws Exception;
}
