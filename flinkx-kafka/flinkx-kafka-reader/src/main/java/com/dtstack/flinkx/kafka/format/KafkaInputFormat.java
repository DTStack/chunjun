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


package com.dtstack.flinkx.kafka.format;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.kafka.client.KafkaConsumer;
import com.dtstack.flinkx.kafkabase.KafkaInputSplit;
import com.dtstack.flinkx.kafkabase.entity.kafkaState;
import com.dtstack.flinkx.kafkabase.enums.KafkaVersion;
import com.dtstack.flinkx.kafkabase.enums.StartupMode;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormat;
import com.dtstack.flinkx.kafkabase.util.KafkaUtil;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.RangeSplitUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.kafka.common.PartitionInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaInputFormat extends KafkaBaseInputFormat {

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        List<kafkaState> stateList = new ArrayList<>();
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(KafkaUtil.geneConsumerProp(consumerSettings, mode));
        if(StartupMode.TIMESTAMP.equals(mode)){
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
            for (PartitionInfo p : partitionInfoList) {
                stateList.add(new kafkaState(p.topic(), p.partition(), null, timestamp));
            }
        }else if(StartupMode.SPECIFIC_OFFSETS.equals(mode)){
            stateList = KafkaUtil.parseSpecificOffsetsString(topic, offset);
        }else{
            String[] topics = topic.split(ConstantValue.COMMA_SYMBOL);
            if(topics.length == 1){
                List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
                for (PartitionInfo p : partitionInfoList) {
                    stateList.add(new kafkaState(p.topic(), p.partition(), null, null));
                }
            }else{
                for (String tp : topics) {
                    List<PartitionInfo> partitionInfoList = consumer.partitionsFor(tp);
                    for (PartitionInfo p : partitionInfoList) {
                        stateList.add(new kafkaState(p.topic(), p.partition(), null, null));
                    }
                }
            }
        }

        List<List<kafkaState>> list = RangeSplitUtil.subListBySegment(stateList, minNumSplits);
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new KafkaInputSplit(i, list.get(i));
        }

        return splits;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        Properties props = KafkaUtil.geneConsumerProp(consumerSettings, mode);
        consumer = new KafkaConsumer(props);
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null && MapUtils.isNotEmpty(stateMap)) {
            KafkaConsumer kafkaConsumer = (KafkaConsumer) this.consumer;
            List<kafkaState> list = new ArrayList<>(stateMap.size());
            for (kafkaState kafkaState : stateMap.values()) {
                list.add(kafkaState.clone());
            }
            kafkaConsumer.submitOffsets(list);
        }
        return formatState;
    }

    @Override
    public KafkaVersion getKafkaVersion() {
        return KafkaVersion.kafka;
    }
}
