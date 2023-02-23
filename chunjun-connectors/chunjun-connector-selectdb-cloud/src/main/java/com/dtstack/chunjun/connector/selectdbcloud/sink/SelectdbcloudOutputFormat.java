package com.dtstack.chunjun.connector.selectdbcloud.sink;

import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConf;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class SelectdbcloudOutputFormat extends BaseRichOutputFormat {

    private SelectdbcloudConf conf;

    private SeletdbcloudWriter writer;

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        // do nothing
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if (rows.isEmpty()) {
            return;
        }
        writer.write(rows);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        writer = new SeletdbcloudWriter(conf);
    }

    @Override
    protected void closeInternal() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public synchronized void writeRecord(RowData rowData) {
        checkTimerWriteException();
        int size = 0;
        rows.add(rowData);
        if (rows.size() >= batchSize) {
            writeRecordInternal();
        }
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
        bytesWriteCounter.add(rowSizeCalculator.getObjectSize(rowData));
        if (checkpointEnabled) {
            snapshotWriteCounter.add(size);
        }
    }

    public void setConf(SelectdbcloudConf conf) {
        this.conf = conf;
    }
}
