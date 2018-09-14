package com.dtstack.flinkx.stream.reader;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2018/09/13 20:00
 */
public class StreamInputFormat extends RichInputFormat {

    protected static final long serialVersionUID = 1L;

    private Row staticData;

    private long recordRead = 0;

    protected long sliceRecordCount;

    protected List<Map<String,Object>> columns;

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        staticData = new Row(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            staticData.setField(i,columns.get(i).get("val"));
        }
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        System.out.println(row.toString());
        recordRead++;
        return staticData;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return recordRead > sliceRecordCount ;
    }

    @Override
    protected void closeInternal() throws IOException {

    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new InputSplit[0];
    }
}
