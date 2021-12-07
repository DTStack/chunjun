package com.dtstack.flinkx.pulsar.client;

import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.pulsar.format.PulsarInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.dtstack.flinkx.pulsar.format.Constants.CONSUMER_SUBSCRIPTION_NAME;
import static com.dtstack.flinkx.pulsar.format.Constants.KEY_PULSAR_SERVICE_URL;

/**
 * The client of pulsar. Avoid confusion to 'org.apache.pulsar.client.api.PulsarClient'
 * Company: www.dtstack.com
 *
 * @author fengjiangtao_yewu@cmss.chinamobile.com 2021/3/29
 */
public class FlinkxPulsarClient implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(FlinkxPulsarClient.class);

    private volatile boolean running = true;
    private boolean blankIgnore;
    private int timeout;
    private String pulsarServiceUrl;
    private IDecode decode;
    private PulsarInputFormat format;
    private Consumer consumer;
    private String listenerName;
    private int batchInterval;
    private int batchBytes;
    private int batchTime;

    public FlinkxPulsarClient(PulsarInputFormat format) {
        this.format = format;
        this.decode = format.getDecode();
        this.blankIgnore = format.getBlankIgnore();
        this.timeout = format.getTimeout();
        this.pulsarServiceUrl = format.getPulsarServiceUrl();
        this.listenerName = format.getListenerName();
        this.batchInterval = format.getBatchInterval();
        this.batchBytes = format.getBatchBytes();
        this.batchTime = format.getBatchTime();

        ClientBuilder clientBuilder;
        try {
            Map<String, Object> consumerSettings = format.getConsumerSettings();
            if (null != format.getToken()) {
                clientBuilder = PulsarClient.builder()
                        .serviceUrl(consumerSettings.get(KEY_PULSAR_SERVICE_URL).toString())
                        .authentication(AuthenticationFactory.token(format.getToken()));
            } else {
                clientBuilder = PulsarClient.builder()
                        .serviceUrl(consumerSettings.get(KEY_PULSAR_SERVICE_URL).toString());
            }

            if (null != format.getListenerName()) {
                clientBuilder.listenerName(listenerName);
            }

            ConsumerBuilder consumerBuilder = clientBuilder.build().newConsumer(Schema.STRING)
                    .topic(format.getTopic())
                    .subscriptionName(CONSUMER_SUBSCRIPTION_NAME)
                    .subscriptionType(SubscriptionType.Failover)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.valueOf(format.getInitialPosition()))
                    .loadConf(consumerSettings);
            if (this.batchInterval > 1) {
                consumerBuilder.batchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(batchInterval)
                        .maxNumBytes(batchBytes).timeout(batchTime, TimeUnit.MILLISECONDS).build());
            }

            consumer = consumerBuilder.subscribe();
        } catch (PulsarClientException e) {
            LOG.error("Pulsar subscribe topic error, topic = {}, e = {}", format.getTopic(), ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException("Pulsar client init error", e);
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> LOG.warn("FlinkxPulsarClient run failed, Throwable = {}", ExceptionUtil.getErrorMessage(e)));

        try {
            while (running) {
                Message msg = consumer.receive(timeout, TimeUnit.MILLISECONDS);
                if (null == msg) {
                    continue;
                }

                String stringMsg = new String(msg.getData());
                try {
                    boolean isIgnoreCurrent = blankIgnore && StringUtils.isBlank(stringMsg);
                    if (isIgnoreCurrent) {
                        continue;
                    }
                    processMessage(stringMsg);
                } catch (Exception e) {
                    LOG.error("Pulsar consumer fetch is error, message = {}, e = {}", new String(msg.getData()), ExceptionUtil.getErrorMessage(e));
                }
                consumer.acknowledgeAsync(msg.getMessageId());
            }
        } catch (Throwable e) {
            LOG.error("Pulsar consumer fetch is error, e = {}", ExceptionUtil.getErrorMessage(e));
        } finally {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                LOG.error("Pulsar consumer close error, e = {}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }


    public void processMessage(String message) {
        Map<String, Object> event;
        if (null == decode) {
            event = fieldDecode(format.getMetaColumns(), format.getFieldDelimiter(), message);
        } else {
            event = decode.decode(message);
        }

        if (event != null && event.size() > 0) {
            format.processEvent(event);
        }
    }

    public void close() {
        try {
            running = false;
            consumer.close();
        } catch (Exception e) {
            LOG.error("close pulsar consumer error", e);
        }
    }

    private Map<String, Object> fieldDecode(List<MetaColumn> metaColumns, String fieldDelimiter, String message) {
        Map<String, Object> event = new HashMap<>();
        String[] valueArr = message.trim().split(fieldDelimiter);

        if (valueArr.length <= 0) {
            return null;
        }

        for (int i = 0; i < valueArr.length; i++) {
            event.put(metaColumns.get(i).getName(), valueArr[i].trim());
        }
        return event;
    }
}
