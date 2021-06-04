package com.dtstack.flinkx.solr.writrer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.solr.SolrConstant;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.List;

public class SolrWriter extends BaseDataWriter {

    private String address;

    private List<MetaColumn> metaColumnList;

    private int batchInterval;


    public SolrWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        address = writerConfig.getParameter().getStringVal(SolrConstant.KEY_ADDRESS);
        batchInterval = writerConfig.getParameter().getIntVal(SolrConstant.KEY_BATCH_SIZE, 1);
        metaColumnList = MetaColumn.getMetaColumns(writerConfig.getParameter().getColumn(), false);

    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        SolrOutputFormatBuilder builder = new SolrOutputFormatBuilder();
        builder.setAddress(address);
        builder.setBatchInterval(batchInterval);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setColumns(metaColumnList);
        return createOutput(dataSet, builder.finish());
    }
}
