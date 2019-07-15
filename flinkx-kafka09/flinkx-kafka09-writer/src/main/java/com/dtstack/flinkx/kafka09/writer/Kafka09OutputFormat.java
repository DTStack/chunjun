package com.dtstack.flinkx.kafka09.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.kafka09.Formatter;
import com.dtstack.flinkx.kafka09.decoder.JsonDecoder;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka09OutputFormat extends RichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka09OutputFormat.class);

    private transient static ObjectMapper objectMapper = new ObjectMapper();

    private Properties props;

    private transient Producer<String, byte[]> producer;

    private String encoding;

    private String timezone;

    private String topic;

    private String brokerList;

    private Map<String, Map<String, String>> topicSelect;

    private Set<Map.Entry<String, Map<String, String>>> entryTopicSelect;

    private Map<String, String> producerSettings;

    private transient JsonDecoder jsonDecoder = new JsonDecoder();

    static {
        Thread.currentThread().setContextClassLoader(null);
    }

    @Override
    public void configure(Configuration parameters) {
        try {
            if (topicSelect != null) {
                entryTopicSelect = topicSelect.entrySet();
            }

            if (props == null) {
                props = new Properties();
                addDefaultKafkaSetting();
            }

            if (producerSettings != null) {
                props.putAll(producerSettings);
            }

            if (!brokerList.trim().equals("")) {
                props.put("metadata.broker.list", brokerList);
            } else {
                throw new Exception("brokerList can not be empty!");
            }

            ProducerConfig pconfig = new ProducerConfig(props);
            if (producer == null) {
                producer = new Producer<String, byte[]>(pconfig);
            }
        } catch (Exception e) {
            LOG.error("", e);
            System.exit(1);
        }
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
        if (row.getArity() == 1){
            Object obj = row.getField(0);
            if(obj instanceof Map) {
                emit((Map<String, Object>) obj);
            } else if (obj instanceof String) {
                emit(jsonDecoder.decode(obj.toString()));
            }
        }
    }

    private void emit(Map<String, Object> event) {
        try {
            String tp = null;
            if(entryTopicSelect != null){
                for(Map.Entry<String,Map<String,String>> entry : entryTopicSelect){
                    String key = entry.getKey();
                    Map<String,String> value = entry.getValue();
                    Set<Map.Entry<String,String>> sets = value.entrySet();
                    for(Map.Entry<String,String> ey:sets){
                        if(ey.getKey().equals(event.get(key))){
                            tp = Formatter.format(event, ey.getValue(), timezone);
                            break;
                        }
                    }
                }
            }
            if(tp==null){
                tp = Formatter.format(event, topic, timezone);
            }
            producer.send(new KeyedMessage<>(tp, event.toString(), objectMapper.writeValueAsString(event).getBytes(encoding)));
        } catch (Exception e) {
            LOG.error("", e);
        }
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

    public void setTopicSelect(Map<String, Map<String, String>> topicSelect) {
        this.topicSelect = topicSelect;
    }

    public void setEntryTopicSelect(Set<Map.Entry<String, Map<String, String>>> entryTopicSelect) {
        this.entryTopicSelect = entryTopicSelect;
    }

    public void setProducerSettings(Map<String, String> producerSettings) {
        this.producerSettings = producerSettings;
    }
}
