package com.dtstack.flinkx.cdc.store;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/1 星期三
 */
public interface Store extends Serializable {

    /** 将ddl数据存储到外部数据源中 */
    void store(RowData data);
}
