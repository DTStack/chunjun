package com.dtstack.flinkx.connector.influxdb.sink;

import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * @Author xirang
 * @Company Dtstack
 * @Date: 2022/3/14 2:57 PM
 */
public class InfluxdbOutputFormat extends BaseRichOutputFormat {
    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {

    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {

    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void closeInternal() throws IOException {

    }
}
