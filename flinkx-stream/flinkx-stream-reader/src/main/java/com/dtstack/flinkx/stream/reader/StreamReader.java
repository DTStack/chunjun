package com.dtstack.flinkx.stream.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

public class StreamReader extends DataReader {

    private long sliceRecordCount;

    private List<Map<String,Object>> columns;

    /** -1 means no limit */
    private static final long DEFAULT_SLICE_RECORD_COUNT = -1;

    public StreamReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        sliceRecordCount = readerConfig.getParameter().getLongVal("sliceRecordCount",DEFAULT_SLICE_RECORD_COUNT);
        columns = readerConfig.getParameter().getColumn();
    }

    @Override
    public DataStream<Row> readData() {
        StreamInputFormatBuilder builder = new StreamInputFormatBuilder();
        builder.setColumns(columns);
        builder.setSliceRecordCount(sliceRecordCount);
        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);
        return createInput(builder.finish(),"streamreader");
    }
}
