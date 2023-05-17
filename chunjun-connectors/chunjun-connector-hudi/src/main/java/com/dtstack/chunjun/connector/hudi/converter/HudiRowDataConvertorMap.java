package com.dtstack.chunjun.connector.hudi.converter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.RowData;

public class HudiRowDataConvertorMap implements MapFunction<RowData, RowData> {
    HudiRawTypeConvertor convertor;

    public HudiRowDataConvertorMap(HudiRawTypeConvertor convertor) {
        this.convertor = convertor;
    }

    @Override
    public RowData map(RowData rowData) throws Exception {
        return convertor.toInternal(rowData);
    }
}
