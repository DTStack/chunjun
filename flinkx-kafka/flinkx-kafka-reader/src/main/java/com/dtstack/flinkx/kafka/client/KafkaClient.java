/*
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.kafka.client;

import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.kafkabase.KafkaInputSplit;
import com.dtstack.flinkx.kafkabase.client.IClient;
import com.dtstack.flinkx.kafkabase.entity.kafkaState;
import com.dtstack.flinkx.kafkabase.enums.StartupMode;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Date: 2019/12/25
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaClient implements IClient {
    protected static Logger LOG = LoggerFactory.getLogger(KafkaClient.class);
    //ck state指针
    private final AtomicReference<Collection<kafkaState>> stateReference;
    private volatile boolean running = true;
    private long pollTimeout;
    private boolean blankIgnore;
    private IDecode decode;
    private KafkaBaseInputFormat format;
    private KafkaConsumer<String, String> consumer;
    //是否触发checkpoint，需要提交offset指针
    private AtomicBoolean commit;

    @SuppressWarnings("unchecked")
    public KafkaClient(Properties clientProps, long pollTimeout, KafkaBaseInputFormat format, KafkaInputSplit kafkaInputSplit) {
        this.pollTimeout = pollTimeout;
        this.blankIgnore = format.getBlankIgnore();
        this.format = format;
        this.decode = format.getDecode();
        this.commit = new AtomicBoolean(false);
        this.stateReference = new AtomicReference<>();
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(clientProps);
        StartupMode mode = format.getMode();
        List<kafkaState> stateList = kafkaInputSplit.getList();
        Map<TopicPartition, Long> partitionMap = new HashMap<>(Math.max((int) (stateList.size()/.75f) + 1, 16));
        Object stateMap = format.getState();
        boolean needToSeek = true;
        if(stateMap instanceof Map && MapUtils.isNotEmpty((Map<String, kafkaState>)stateMap)){
            Map<String, kafkaState> map = (Map<String, kafkaState>) stateMap;
            for (kafkaState state : map.values()) {
                TopicPartition tp = new TopicPartition(state.getTopic(), state.getPartition());
                //ck中保存的是当前已经读取的offset，恢复时从下一条开始读
                partitionMap.put(tp, state.getOffset() + 1);
            }
            LOG.info("init kafka client from [checkpoint], stateMap = {}", map);
        }else if(CollectionUtils.isEmpty(stateList)){
            running = false;
            LOG.warn("\n" +
                    "****************************************************\n" +
                    "*******************    WARN    *********************\n" +
                    "| this stateList in KafkaInputSplit is empty,      |\n" +
                    "| this channel will not assign any kafka topic,    |\n" +
                    "| therefore, no data will be read in this channel! |\n" +
                    "****************************************************");
            return;
        }if(StartupMode.TIMESTAMP.equals(mode)){
            Map<TopicPartition, Long> timestampMap = new HashMap<>(Math.max((int) (stateList.size()/.75f) + 1, 16));
            for (kafkaState state : stateList) {
                TopicPartition tp = new TopicPartition(state.getTopic(), state.getPartition());
                timestampMap.put(tp,  state.getTimestamp());
                partitionMap.put(tp, null);
            }
            Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampMap);
            for (TopicPartition tp : partitionMap.keySet()) {
                OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
                if (offsetAndTimestamp != null) {
                    partitionMap.put(tp, offsetAndTimestamp.offset());
                }
            }
            LOG.info("init kafka client from [timestamp], offsets = {}", offsets);
        }else if(StartupMode.SPECIFIC_OFFSETS.equals(mode)){
            for (kafkaState state : stateList) {
                TopicPartition tp = new TopicPartition(state.getTopic(), state.getPartition());
                partitionMap.put(tp, state.getOffset());
            }
            LOG.info("init kafka client from [specific-offsets], stateList = {}", stateList);
        }else{
            for (kafkaState state : stateList) {
                partitionMap.put(new TopicPartition(state.getTopic(), state.getPartition()), null);
            }
            needToSeek = false;
            LOG.info("init kafka client from [split], stateList = {}", stateList);
        }
        LOG.info("partitionList = {}", partitionMap.keySet());
        consumer.assign(partitionMap.keySet());
        if(needToSeek){
            for (Map.Entry<TopicPartition, Long> entry : partitionMap.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> {
            LOG.error("KafkaClient run failed, Throwable = {}", ExceptionUtil.getErrorMessage(e));
        });
        try {
            while (running) {
                if(this.commit.getAndSet(false)){
                    final Collection<kafkaState> kafkaStates = stateReference.getAndSet(null);
                    if(kafkaStates != null){
                        LOG.info("submit kafka offset, kafkaStates = {}", kafkaStates);
                        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(Math.max((int) (kafkaStates.size()/.75f) + 1, 16));
                        for (kafkaState state : kafkaStates) {
                            offsets.put(new TopicPartition(state.getTopic(), state.getPartition()), new OffsetAndMetadata(state.getOffset(), "no metadata"));
                        }
                        try {
                            consumer.commitAsync(offsets, (o, ex) -> {
                                if (ex != null) {
                                    LOG.warn("Committing offsets to Kafka failed, This does not compromise Flink's checkpoints. offsets = {}, e = {}", o, ExceptionUtil.getErrorMessage(ex));
                                } else {
                                    LOG.info("Committing offsets to Kafka async successfully, offsets = {}", o);
                                }
                            });
                        }catch (Exception e){
                            LOG.warn("Committing offsets to Kafka failed, This does not compromise Flink's checkpoints. offsets = {}, e = {}", offsets, ExceptionUtil.getErrorMessage(e));
                            try {
                                consumer.commitSync(offsets);
                                LOG.info("Committing offsets to Kafka successfully, offsets = {}", offsets);
                            }catch (Exception e1){
                                LOG.warn("Committing offsets to Kafka failed, This does not compromise Flink's checkpoints. offsets = {}, e = {}", offsets, ExceptionUtil.getErrorMessage(e1));
                            }
                        }
                    }
                }

                ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, String> r : records) {
                    boolean isIgnoreCurrent = r.value() == null || blankIgnore && StringUtils.isBlank(r.value());
                    if (isIgnoreCurrent) {
                        continue;
                    }
                    
                    try {
                        processMessage(r.value(), r.topic(), r.partition(), r.offset(), r.timestamp());
                    } catch (Throwable e) {
                        LOG.warn("kafka consumer fetch is error, message:{}, e = {}", r.value(), ExceptionUtil.getErrorMessage(e));
                    }
                }
            }
        } catch (WakeupException e) {
            LOG.warn("WakeupException to close kafka consumer, e = {}", ExceptionUtil.getErrorMessage(e));
        } catch (Throwable e) {
            LOG.warn("kafka consumer fetch is error, e = {}", ExceptionUtil.getErrorMessage(e));
        } finally {
            consumer.close();
        }
    }

    @Override
    public void processMessage(String message, String topic, Integer partition, Long offset, Long timestamp) {
        Map<String, Object> event = decode.decode(message);
        if (event != null && event.size() > 0) {
            format.processEvent(Pair.of(event, new kafkaState(topic, partition, offset, timestamp)));
        }
    }

    /**
     * 提交kafka offset
     * @param kafkaStates
     */
    public void submitOffsets(Collection<kafkaState> kafkaStates){
        this.commit.set(true);
        this.stateReference.getAndSet(kafkaStates);
    }

    @Override
    public void close() {
        try {
            running = false;
            consumer.wakeup();
        } catch (Exception e) {
            LOG.error("close kafka consumer error, e = {}", ExceptionUtil.getErrorMessage(e));
        }
    }

}
