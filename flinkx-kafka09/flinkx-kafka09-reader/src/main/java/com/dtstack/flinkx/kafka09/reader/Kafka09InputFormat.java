package com.dtstack.flinkx.kafka09.reader;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka09InputFormat extends RichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka09InputFormat.class);

    private Map<String, ConsumerConnector> consumerConnMap = new HashMap<>();

    private String encoding;

    private String codec;

    private List<String> topics;

    private Map<String, String> consumerSettings;

    private BlockingQueue<Row> queue;

    private transient ExecutorService executor;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        for (String topic : topics) {
            ConsumerConnector consumer = consumerConnMap.get(topic);
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                executor.submit(new KafkaConsumer(stream, this));
            }
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
        for (ConsumerConnector consumer : consumerConnMap.values()) {
            consumer.commitOffsets(true);
            consumer.shutdown();
        }

        executor.shutdownNow();
    }

    @Override
    public void configure(Configuration parameters) {


        Properties props = geneConsumerProp();
        for (String topicName : topics) {
            ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
            consumerConnMap.put(topicName, consumer);
        }

        executor = Executors.newFixedThreadPool(topics.size());
        queue = new SynchronousQueue<Row>(false);

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

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(Row.of(event));
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, e);
        }
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getEncoding() {
        return encoding;
    }

    public String getCodec() {
        return codec;
    }

    public void setCodec(String codec) {
        this.codec = codec;
    }

    public void setTopic(List<String> topics) {
        this.topics = topics;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings) {
        this.consumerSettings = consumerSettings;
    }

}
