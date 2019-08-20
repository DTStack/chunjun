package com.dtstack.flinkx.kafka10.writer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by daguan on 18/9/3.
 */
public class KafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(100);

    private org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;

    public KafkaProducer(Properties props) {
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    public static KafkaProducer init(Properties p) {

        Properties props = new Properties();

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.timeout.ms", "86400000");
        props.put("retries", "1000000");
        props.put("max.in.flight.requests.per.connection", "1");

        if(p != null) {
            props.putAll(p);
        }

        return new KafkaProducer(props);
    }

    public static KafkaProducer init(String bootstrapServers) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        return init(props);
    }

    /**
     * 发送消息，失败重试。
     * @param topic
     * @param key
     * @param value
     */
    public void sendWithRetry(String topic, String key, String value) {
        while(!queue.isEmpty()) {
            sendWithBlock(topic, key, queue.poll());
        }

        sendWithBlock(topic, key, value);
    }


    /**
     * 发送消息，失败阻塞（放到有界阻塞队列）。
     * @param topic
     * @param key
     * @param value
     */
    public void sendWithBlock(String topic, String key, final String value) {

        if(value == null) {
            return;
        }

        producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                try {

                    if (exception != null) {
                        queue.put(value);
                        logger.error("send data failed, wait to retry, value={},error={}", value, exception.getMessage());
                        Thread.sleep(1000l);
                    }
                } catch (InterruptedException e) {
                    logger.error("kafka send callback error",e);
                }

            }
        });

    }

    public void close() {
        producer.close();
    }

    public void flush() {
        producer.flush();
    }

}

