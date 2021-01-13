/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.kafkabase.format;

import com.dtstack.flinkx.decoder.DecodeEnum;
import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.decoder.TextDecoder;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.kafkabase.KafkaInputSplit;
import com.dtstack.flinkx.kafkabase.client.KafkaBaseConsumer;
import com.dtstack.flinkx.kafkabase.entity.kafkaState;
import com.dtstack.flinkx.kafkabase.enums.KafkaVersion;
import com.dtstack.flinkx.kafkabase.enums.StartupMode;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaBaseInputFormat extends BaseRichInputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaBaseInputFormat.class);

    protected String topic;
    protected String groupId;
    protected String codec;
    protected boolean blankIgnore;
    protected StartupMode mode;
    protected String offset;
    protected Long timestamp;
    protected Map<String, String> consumerSettings;
    protected List<MetaColumn> metaColumns;
    protected Map<String, kafkaState> stateMap;
    protected volatile boolean running = false;
    protected transient BlockingQueue<Row> queue;
    protected transient KafkaBaseConsumer consumer;
    protected transient IDecode decode;


    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        queue = new SynchronousQueue<>(false);
        stateMap = new HashMap<>(16);
        if (DecodeEnum.JSON.getName().equalsIgnoreCase(codec)) {
            decode = new JsonDecoder();
        } else {
            decode = new TextDecoder();
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        LOG.info("inputSplit = {}", inputSplit);
        consumer.createClient(topic, groupId, this, (KafkaInputSplit) inputSplit).execute();
        running = true;
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

    public void processEvent(Pair<Map<String, Object>, kafkaState> pair) {
        try {
            Row row;
            if(CollectionUtils.isEmpty(metaColumns)){
                row = Row.of(Row.of(pair.getLeft()));
            }else{
                row = new Row(metaColumns.size());
                for (int i = 0; i < metaColumns.size(); i++) {
                    MetaColumn metaColumn = metaColumns.get(i);
                    Object value = pair.getLeft().get(metaColumn.getName());
                    Object obj = StringUtil.string2col(String.valueOf(value), metaColumn.getType(), metaColumn.getTimeFormat());
                    row.setField(i , obj);
                }
            }
            queue.put(row);

            kafkaState state = pair.getRight();
            stateMap.put(String.format("%s-%s", state.getTopic(), state.getPartition()), state);
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted pair:{} error:{}", pair, e);
        }
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null) {
            formatState.setState(stateMap);
        }
        return formatState;
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    @Override
    protected void closeInternal() {
        if (running) {
            consumer.close();
            running = false;
            LOG.warn("input kafka release.");
        }
    }

    /**
     * 获取kafka版本信息
     *  0.9:  kakfa09
     *  0.10: kafka10
     * @return
     */
    public KafkaVersion getKafkaVersion() {
        return KafkaVersion.unknown;
    }

    /**
     * 获取kafka state
     * @return
     */
    public Object getState(){
        return formatState == null ? null : formatState.getState();
    }

    public IDecode getDecode() {
        return decode;
    }

    public boolean getBlankIgnore() {
        return blankIgnore;
    }

    public StartupMode getMode() {
        return mode;
    }


}
