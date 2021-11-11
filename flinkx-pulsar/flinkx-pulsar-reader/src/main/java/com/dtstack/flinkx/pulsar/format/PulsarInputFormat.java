package com.dtstack.flinkx.pulsar.format;

import com.dtstack.flinkx.decoder.DecodeEnum;
import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.decoder.TextDecoder;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.pulsar.client.FlinkxPulsarConsumer;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * The inputFormat of PulsarReader
 * <p>
 * Company: www.dtstack.com
 *
 * @author fengjiangtao_yewu@cmss.chinamobile.com 2021-3-23
 */
public class PulsarInputFormat extends BaseRichInputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(PulsarInputFormat.class);

    protected String token;
    protected String topic;
    protected String codec;
    protected String initialPosition;
    protected String fieldDelimiter;
    protected boolean blankIgnore;
    protected int timeout;
    protected List<MetaColumn> metaColumns;
    protected Map<String, Object> consumerSettings;
    protected String listenerName;

    protected volatile boolean running = false;
    protected transient FlinkxPulsarConsumer flinkxPulsarConsumer;
    protected transient BlockingQueue<Row> queue;
    protected transient IDecode decode;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        queue = new SynchronousQueue<>(false);
        if (DecodeEnum.JSON.getName().equalsIgnoreCase(codec)) {
            decode = new JsonDecoder();
        } else if (DecodeEnum.TEXT.getName().equalsIgnoreCase(codec)) {
            decode = new TextDecoder();
        } else {
            decode = null;
        }

        flinkxPulsarConsumer = new FlinkxPulsarConsumer();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        LOG.info("inputSplit = {}", inputSplit);
        flinkxPulsarConsumer.createClient(this).execute();
        running = true;
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }

        return splits;
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }

        return row;
    }

    @Override
    protected void closeInternal() {
        if (running) {
            flinkxPulsarConsumer.close();
            running = false;
            LOG.warn("input pulsar release.");
        }
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    public void processEvent(Map<String, Object> event) {
        try {
            Row row;
            if (CollectionUtils.isEmpty(metaColumns)) {
                row = Row.of(event);
            } else {
                row = new Row(metaColumns.size());
                for (int i = 0; i < metaColumns.size(); i++) {
                    MetaColumn metaColumn = metaColumns.get(i);
                    Object value = event.get(metaColumn.getName());
                    Object obj = StringUtil.string2col(String.valueOf(value), metaColumn.getType(), metaColumn.getTimeFormat());
                    row.setField(i, obj);
                }
            }

            queue.put(row);
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, e);
        }
    }

    public String getToken() {
        return token;
    }

    public String getTopic() {
        return topic;
    }

    public IDecode getDecode() {
        return decode;
    }

    public boolean getBlankIgnore() {
        return blankIgnore;
    }

    public String getInitialPosition() {
        return initialPosition;
    }

    public int getTimeout() {
        return timeout;
    }

    public Map<String, Object> getConsumerSettings() {
        return consumerSettings;
    }

    public String getListenerName() {
        return listenerName;
    }

    public List<MetaColumn> getMetaColumns() {
        return metaColumns;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }
}
