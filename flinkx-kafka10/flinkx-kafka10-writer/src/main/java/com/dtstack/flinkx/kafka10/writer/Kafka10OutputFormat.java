package com.dtstack.flinkx.kafka10.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.kafka10.Formatter;
import com.dtstack.flinkx.kafka10.decoder.JsonDecoder;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka10OutputFormat extends RichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka10OutputFormat.class);

    private transient static ObjectMapper objectMapper = new ObjectMapper();

    private Properties props;

    private String timezone;

    private String topic;

    private Map<String, Map<String, String>> topicSelect;

    private Set<Map.Entry<String, Map<String, String>>> entryTopicSelect;

    private String bootstrapServers;

    private Map<String, String> producerSettings;

    private transient KafkaProducer producer;

    private transient JsonDecoder jsonDecoder = new JsonDecoder();

    private AtomicBoolean isInit = new AtomicBoolean(false);

    @Override
    public void configure(Configuration parameters) {
        try {
            if (topicSelect != null) {
                entryTopicSelect = topicSelect.entrySet();
            }
            if (props == null) {
                props = new Properties();
            }
            if (producerSettings != null) {
                props.putAll(producerSettings);
            }
            if (!bootstrapServers.trim().equals("")) {
                props.put("bootstrap.servers", bootstrapServers);
            } else {
                throw new Exception("bootstrapServers can not be empty!");
            }
            if (isInit.compareAndSet(false, true)) {
                producer = KafkaProducer.init(props);
            }
        } catch (Exception e) {
            LOG.error("kafka producer init error", e);
            System.exit(1);
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    public void closeInternal() throws IOException {
        producer.close();
        LOG.warn("output kafka release.");
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if (row.getArity() == 1) {
            Object obj = row.getField(0);
            if (obj instanceof Map) {
                emit((Map<String, Object>) obj);
            } else if (obj instanceof String) {
                emit(jsonDecoder.decode(obj.toString()));
            }
        }
    }

    private void emit(Map event) {
        try {
            String tp = null;
            if (entryTopicSelect != null) {
                for (Map.Entry<String, Map<String, String>> entry : entryTopicSelect) {
                    String key = entry.getKey();
                    Map<String, String> value = entry.getValue();
                    Set<Map.Entry<String, String>> sets = value.entrySet();
                    for (Map.Entry<String, String> ey : sets) {
                        if (ey.getKey().equals(event.get(key))) {
                            tp = Formatter.format(event, ey.getValue(), timezone);
                            break;
                        }
                    }
                }
            }
            if (tp == null) {
                tp = Formatter.format(event, topic, timezone);
            }

            producer.sendWithRetry(tp, event.toString(), objectMapper.writeValueAsString(event));
        } catch (Exception e) {
            LOG.error("kafka output error to block error:{}", e);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        throw new UnsupportedOperationException();
    }


    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setTopicSelect(Map<String, Map<String, String>> topicSelect) {
        this.topicSelect = topicSelect;
    }

    public void setEntryTopicSelect(Set<Map.Entry<String, Map<String, String>>> entryTopicSelect) {
        this.entryTopicSelect = entryTopicSelect;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setProducerSettings(Map<String, String> producerSettings) {
        this.producerSettings = producerSettings;
    }
}
