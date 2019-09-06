package com.dtstack.flinkx.kafka09.writer;

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.kafka09.Formatter;
import com.dtstack.flinkx.kafka09.decoder.JsonDecoder;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka09OutputFormat extends RichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka09OutputFormat.class);

    private Properties props;

    private String encoding;

    private String timezone;

    private String topic;

    private String brokerList;

    private Map<String, String> producerSettings;

    private transient Producer<String, byte[]> producer;

    private transient JsonDecoder jsonDecoder = new JsonDecoder();

    private transient static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Configuration parameters) {
        props = new Properties();
        addDefaultKafkaSetting();
        if (producerSettings != null) {
            props.putAll(producerSettings);
        }
        if (StringUtils.isBlank(brokerList)) {
            throw new RuntimeException("brokerList can not be empty!");
        }
        props.put("metadata.broker.list", brokerList);

        ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<String, byte[]>(producerConfig);
    }

    private void addDefaultKafkaSetting() {
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("value.serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("producer.type", "sync");
        props.put("compression.codec", "none");
        props.put("request.required.acks", "1");
        props.put("batch.num.messages", "1024");
        props.put("client.id", "");
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        try {
            if (row.getArity() == 1) {
                Object obj = row.getField(0);
                if (obj != null && obj instanceof Map) {
                    emit((Map<String, Object>) obj);
                } else if (obj instanceof String) {
                    emit(jsonDecoder.decode(obj.toString()));
                }
            }
        } catch (Throwable e) {
            LOG.error("kafka writeSingleRecordInternal error:{}", e);
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    private void emit(Map<String, Object> event) throws IOException {
        String tp = Formatter.format(event, topic, timezone);
        producer.send(new KeyedMessage<>(tp, event.toString(), objectMapper.writeValueAsString(event).getBytes(encoding)));
    }

    @Override
    public void closeInternal() throws IOException {
        LOG.warn("kafka output closeInternal.");
        producer.close();
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        throw new UnsupportedOperationException();
    }


    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public void setProducerSettings(Map<String, String> producerSettings) {
        this.producerSettings = producerSettings;
    }

    public void setRestoreConfig(RestoreConfig restoreConfig) {
        this.restoreConfig = restoreConfig;
    }
}
