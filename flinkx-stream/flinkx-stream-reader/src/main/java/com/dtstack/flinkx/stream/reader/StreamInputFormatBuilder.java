package com.dtstack.flinkx.stream.reader;

import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;

import java.util.List;
import java.util.Map;

public class StreamInputFormatBuilder extends RichInputFormatBuilder {

    private StreamInputFormat format;

    public StreamInputFormatBuilder() {
        super.format = format = new StreamInputFormat();
    }

    public void setSliceRecordCount(long sliceRecordCount){
        format.sliceRecordCount = sliceRecordCount;
    }

    public void setColumns(List<Map<String,Object>> columns){
        format.columns = columns;
    }

    @Override
    protected void checkFormat() {
        if (format.columns == null || format.columns.size() == 0){
            throw new IllegalArgumentException("columns can not be empty");
        }
    }
}
