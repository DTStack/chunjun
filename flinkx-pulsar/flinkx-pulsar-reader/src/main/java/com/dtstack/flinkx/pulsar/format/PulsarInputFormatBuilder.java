package com.dtstack.flinkx.pulsar.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * The builder of pulsar inputFormat
 * Company: www.dtstack.com
 *
 * @author fengjiangtao_yewu@cmss.chinamobile.com 2021/3/23
 */
public class PulsarInputFormatBuilder extends BaseRichInputFormatBuilder {

    private PulsarInputFormat format;

    public PulsarInputFormatBuilder(PulsarInputFormat format) {
        super.format = this.format = format;
    }

    public void setToken(String token) {
        format.token = token;
    }

    public void setTopic(String topic) {
        format.topic = topic;
    }

    public void setCodec(String codec) {
        format.codec = codec;
    }

    public void setConsumerSettings(Map<String, Object> consumerSettings) {
        format.consumerSettings = consumerSettings;
    }

    public void setListenerName(String listenerName) {
        format.listenerName = listenerName;
    }

    public void setMetaColumns(List<MetaColumn> metaColumns) {
        format.metaColumns = metaColumns;
    }

    public void setBlankIgnore(boolean blankIgnore) {
        format.blankIgnore = blankIgnore;
    }

    public void setInitialPosition(String initialPosition) {
        format.initialPosition = initialPosition;
    }

    public void setTimeout(int timeout) {
        format.timeout = timeout;
    }

    public void setPulsarServiceUrl(String pulsarServiceUrl) {
        format.pulsarServiceUrl = pulsarServiceUrl;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        format.fieldDelimiter = fieldDelimiter;
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(128);
        if (StringUtils.isBlank(format.topic)) {
            sb.append("No pulsar topic supplied;\n");
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
