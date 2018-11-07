package com.dtstack.flinkx.carbondata.reader;


import com.dtstack.flinkx.inputformat.RichInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;

public class CarbondataInputFormat extends RichInputFormat {
    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {

    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return null;
    }

    @Override
    protected void closeInternal() throws IOException {

    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public InputSplit[] createInputSplits(int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }
}
