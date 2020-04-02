package com.dtstack.flinkx.pulsar.writer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;

import java.util.Map;

/**
 * @author: pierre
 * @create: 2020/3/21
 */
public class PulsarOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private PulsarOutputFormat format;

    public PulsarOutputFormatBuilder() {
        super.format = format = new PulsarOutputFormat();
    }

    public void setTopic(String topic) {
        format.topic = topic;
    }

    public void setToken(String token) {
        format.token = token;
    }

    public void setPulsarServiceUrl(String pulsarServiceUrl) {
        format.pulsarServiceUrl = pulsarServiceUrl;
    }

    public void setProducerSettings(Map<String, Object> producerSettings) {
        format.producerSettings = producerSettings;
    }

    @Override
    protected void checkFormat() {

    }
}
