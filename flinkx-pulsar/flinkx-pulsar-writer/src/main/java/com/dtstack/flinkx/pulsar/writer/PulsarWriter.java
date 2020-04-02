package com.dtstack.flinkx.pulsar.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.writer.BaseDataWriter;
import static com.dtstack.flinkx.pulsar.writer.Constants.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;


/**
 * @author: pierre
 * @create: 2020/3/21
 */
public class PulsarWriter extends BaseDataWriter {
    protected String topic;
    protected String token;
    protected String pulsarServiceUrl;
    protected List<String> tableFields;
    protected Map<String, Object> producerSettings;

    public PulsarWriter(DataTransferConfig config){
        super(config);
        topic = config.getJob().getContent().get(0).getWriter().getParameter().getStringVal(KEY_TOPIC);
        token = config.getJob().getContent().get(0).getWriter().getParameter().getStringVal(KEY_TOKEN);
        pulsarServiceUrl = config.getJob().getContent().get(0).getWriter().getParameter().getStringVal(KEY_PULSAR_SERVICE_URL);
        producerSettings = (Map<String, Object>) config.getJob().getContent().get(0).getWriter().getParameter().getVal(KEY_PRODUCER_SETTINGS);
        tableFields = (List<String>)config.getJob().getContent().get(0).getWriter().getParameter().getVal(KEY_TABLE_FIELDS);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        PulsarOutputFormatBuilder builder = new PulsarOutputFormatBuilder();
        builder.setTopic(topic);
        builder.setPulsarServiceUrl(pulsarServiceUrl);
        builder.setProducerSettings(producerSettings);
        builder.setToken(token);
        return createOutput(dataSet, builder.finish());
    }
}
