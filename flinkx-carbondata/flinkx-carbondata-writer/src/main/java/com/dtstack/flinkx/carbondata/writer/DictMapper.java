package com.dtstack.flinkx.carbondata.writer;


import com.dtstack.flinkx.carbondata.writer.dict.CarbonDictionaryUtil;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class DictMapper extends RichMapFunction<Row,Row> {

    private List<String[]> data = new ArrayList<>();

    public DictMapper(CarbonTable table, List<String> columns) {

    }

    @Override
    public Row map(Row row) throws Exception {
        return row;
    }

    @Override
    public void close() throws Exception {
        if(!data.isEmpty()) {
            //CarbonDictionaryUtil.generateGlobalDictionary();
        }
    }

}
