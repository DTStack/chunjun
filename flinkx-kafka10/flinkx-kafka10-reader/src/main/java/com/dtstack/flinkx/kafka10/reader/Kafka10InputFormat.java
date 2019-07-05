package com.dtstack.flinkx.kafka10.reader;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.kafka10.decoder.IDecode;
import com.dtstack.flinkx.kafka10.decoder.JsonDecoder;
import com.dtstack.flinkx.kafka10.decoder.PlainDecoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka10InputFormat extends RichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka10InputFormat.class);

    private static int threadCount = 1;

    private String topic;

    private String groupId;

    private String codec;

    private String bootstrapServers;

    private Map<String, String> consumerSettings;

    private KafkaConsumer consumer;

    private IDecode decode;

    private BlockingQueue<Row> queue;

    static {
        Thread.currentThread().setContextClassLoader(null);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            consumer.add(topic, groupId, new KafkaConsumer.Caller() {

                @Override
                public void processMessage(String message) {
                    Map<String, Object> event = Kafka10InputFormat.this.getDecode().decode(message);
                    if (event != null && event.size() > 0) {
                        Kafka10InputFormat.this.processEvent(event);
                    }
                }

                @Override
                public void catchException(String message, Throwable e) {
                    LOG.warn("kakfa consumer fetch is error,message={}", message);
                    LOG.error("kakfa consumer fetch is error", e);
                }
            }, Integer.MAX_VALUE, threadCount).execute();

        } catch (Exception e) {
            LOG.error("kafka emit error", e);
        }
    }

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(Row.of(event));
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, e);
        }
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", e);
        }
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        consumer.close();
        LOG.warn("input kafka release.");
    }

    @Override
    public void configure(Configuration parameters) {
        Properties props = geneConsumerProp();

        props.put("bootstrap.servers", bootstrapServers);

        consumer = KafkaConsumer.init(props);

        queue = new SynchronousQueue<Row>(false);

        decode = createDecoder();
    }

    private Properties geneConsumerProp() {
        Properties props = new Properties();

        Iterator<Map.Entry<String, String>> consumerSetting = consumerSettings
                .entrySet().iterator();

        while (consumerSetting.hasNext()) {
            Map.Entry<String, String> entry = consumerSetting.next();
            String k = entry.getKey();
            String v = entry.getValue();
            props.put(k, v);
        }

        return props;
    }

    private IDecode createDecoder() {
        if ("json".equals(codec)) {
            return new JsonDecoder();
        } else {
            return new PlainDecoder();
        }
    }

    public IDecode getDecode() {
        return decode;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setCodec(String codec) {
        this.codec = codec;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings) {
        this.consumerSettings = consumerSettings;
    }
}
