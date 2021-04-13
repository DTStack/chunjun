package com.dtstack.flinkx.converter;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * 类型T一般是 Object，HBase这种特殊的就是byte[]
 * @param <T>
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/13
 **/
@FunctionalInterface
interface SerializationConverter<T> extends Serializable {
    void serialize(RowData rowData, int pos, T output) throws Exception;
}
