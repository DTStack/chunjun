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

package com.dtstack.chunjun.connector.kafka.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.kafka.adapter.StartupModeAdapter;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConfig;
import com.dtstack.chunjun.connector.kafka.converter.KafkaRawTypeMapping;
import com.dtstack.chunjun.connector.kafka.converter.KafkaSyncConverter;
import com.dtstack.chunjun.connector.kafka.enums.StartupMode;
import com.dtstack.chunjun.connector.kafka.serialization.RowDeserializationSchema;
import com.dtstack.chunjun.connector.kafka.util.FormatNameConvertUtil;
import com.dtstack.chunjun.connector.kafka.util.KafkaUtil;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.PluginUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KafkaSourceFactory extends SourceFactory {

    /** kafka conf */
    protected KafkaConfig kafkaConfig;

    public KafkaSourceFactory(SyncConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(StartupMode.class, new StartupModeAdapter())
                        .create();
        GsonUtil.setTypeAdapter(gson);
        kafkaConfig =
                gson.fromJson(gson.toJson(config.getReader().getParameter()), KafkaConfig.class);
        kafkaConfig.setColumn(config.getReader().getFieldList());

        if (MapUtils.isNotEmpty(kafkaConfig.getTableSchema())) {
            HashMap<String, List<FieldConfig>> stringListHashMap = new HashMap<>();
            for (Map.Entry<String, List> next :
                    ((Map<String, List>) config.getReader().getParameter().get("tableSchema"))
                            .entrySet()) {
                List<FieldConfig> fieldConfigs = syncConfig.getReader().getFieldList();
                stringListHashMap.put(next.getKey(), fieldConfigs);
            }
            kafkaConfig.setTableSchema(stringListHashMap);
        }
        super.initCommonConf(kafkaConfig);
        registerFormatPluginJar();
    }

    private void registerFormatPluginJar() {

        try {
            Set<URL> urlSet = getExtraUrl();

            if (urlSet.size() > 0) {
                List<String> urlList = PluginUtil.registerPluginUrlToCachedFile(env, urlSet);
                syncConfig
                        .getSyncJarList()
                        .addAll(
                                (PluginUtil.setPipelineOptionsToEnvConfig(
                                        env, urlList, syncConfig.getMode())));
            }
        } catch (Exception e) {
            throw new RuntimeException("register plugin failed", e);
        }
    }

    @Override
    public DataStream<RowData> createSource() {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("kafka not support transform");
        }
        List<String> topics = Lists.newArrayList(kafkaConfig.getTopic());
        if (CollectionUtils.isNotEmpty(kafkaConfig.getTopics())) {
            topics = kafkaConfig.getTopics();
        }
        Properties props = new Properties();
        props.put("group.id", kafkaConfig.getGroupId());
        props.putAll(kafkaConfig.getConsumerSettings());
        RowType rowType =
                TableUtil.createRowType(kafkaConfig.getColumn(), KafkaRawTypeMapping::apply);
        DynamicKafkaDeserializationSchema deserializationSchema =
                new RowDeserializationSchema(
                        kafkaConfig, new KafkaSyncConverter(rowType, kafkaConfig));
        KafkaConsumerWrapper consumer =
                new KafkaConsumerWrapper(topics, deserializationSchema, props);
        switch (kafkaConfig.getMode()) {
            case EARLIEST:
                consumer.setStartFromEarliest();
                break;
            case LATEST:
                consumer.setStartFromLatest();
                break;
            case TIMESTAMP:
                consumer.setStartFromTimestamp(kafkaConfig.getTimestamp());
                break;
            case SPECIFIC_OFFSETS:
                consumer.setStartFromSpecificOffsets(
                        KafkaUtil.parseSpecificOffsetsString(
                                kafkaConfig.getTopics(), kafkaConfig.getOffset()));
                break;
            default:
                consumer.setStartFromGroupOffsets();
                break;
        }
        consumer.setCommitOffsetsOnCheckpoints(kafkaConfig.getGroupId() != null);
        return createInput(consumer, syncConfig.getReader().getName());
    }

    public Set<URL> getExtraUrl() {
        String deserialization = kafkaConfig.getDeserialization();
        if (StringUtils.isNotBlank(deserialization)) {
            String pluginPath =
                    com.dtstack.chunjun.constants.ConstantValue.FORMAT_DIR_NAME
                            + File.separator
                            + FormatNameConvertUtil.convertPackageName(deserialization);

            Set<URL> urlSet =
                    PluginUtil.getJarFileDirPath(
                            pluginPath,
                            syncConfig.getPluginRoot(),
                            syncConfig.getRemotePluginPath(),
                            "");

            return urlSet;
        }
        return Collections.emptySet();
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return KafkaRawTypeMapping::apply;
    }
}
