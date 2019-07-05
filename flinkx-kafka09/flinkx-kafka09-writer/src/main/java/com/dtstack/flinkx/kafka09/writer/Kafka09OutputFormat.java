package com.dtstack.flinkx.kafka09.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka09OutputFormat extends RichOutputFormat {
    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {

    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {

    }
}
