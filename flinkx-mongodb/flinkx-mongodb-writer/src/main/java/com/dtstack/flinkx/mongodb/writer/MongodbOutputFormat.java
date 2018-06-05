package com.dtstack.flinkx.mongodb.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @author jiangbo
 * @date 2018/6/5 21:17
 */
public class MongodbOutputFormat extends RichOutputFormat {



    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        // TODO
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        for (Row row : rows) {

        }
    }
}
