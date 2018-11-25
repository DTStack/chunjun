package com.dtstack.flinkx.carbondata.writer;

import com.dtstack.flinkx.carbondata.CarbonConfigKeys;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;
import java.util.List;
import java.util.Map;



/**
 * Carbondata writer plugin
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataWriter extends DataWriter {

    protected DatabaseInterface databaseInterface;

    protected String table;

    protected String database;

    protected String path;

    protected Map<String,String> hadoopConfig;

    protected List<String> column;

    protected int batchSize;

    public CarbondataWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        hadoopConfig = (Map<String, String>) writerConfig.getParameter().getVal(CarbonConfigKeys.KEY_HADOOP_CONFIG);
        table = writerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_TABLE);
        database = writerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_DATABASE);
        path = writerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_TABLE_PATH);
        column = (List<String>) writerConfig.getParameter().getColumn();
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        CarbondataOutputFormatBuilder builder = new CarbondataOutputFormatBuilder();
        builder.setColumn(column);
        builder.setDatabase(database);
        builder.setTable(table);
        builder.setPath(path);
        builder.setHadoopConfig(hadoopConfig);
        builder.setBatchInterval(batchSize);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setErrorRatio(errorRatio);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        OutputFormatSinkFunction sinkFunction = new OutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);
        String sinkName = "carbonwriter";
        dataStreamSink.name(sinkName);
        return dataStreamSink;
    }

}

