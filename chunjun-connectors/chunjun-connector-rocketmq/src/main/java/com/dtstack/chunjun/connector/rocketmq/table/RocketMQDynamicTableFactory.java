/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.rocketmq.table;

import com.dtstack.chunjun.connector.rocketmq.config.RocketMQConfig;
import com.dtstack.chunjun.connector.rocketmq.source.RocketMQScanTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.CONSUMER_GROUP;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.NAME_SERVER_ADDRESS;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_ACCESS_CHANNEL;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_ACCESS_KEY;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_CONSUMER_BATCH_SIZE;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_ENCODING;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_END_TIME;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_HEART_BEAT_BROKER_INTERVAL;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_PERSIST_CONSUMER_INTERVAL;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_SECRET_KEY;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_START_MESSAGE_OFFSET;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_START_MESSAGE_TIMESTAMP;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_START_OFFSET_MODE;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_START_TIME;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_START_TIME_MILLS;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_TAG;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.OPTIONAL_TIME_ZONE;
import static com.dtstack.chunjun.connector.rocketmq.table.RocketMQOptions.TOPIC;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

public class RocketMQDynamicTableFactory implements DynamicTableSourceFactory {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String IDENTIFIER = "rocketmq-x";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        transformContext(this, context);
        final FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        Integer parallelism = config.get(SCAN_PARALLELISM);

        return new RocketMQScanTableSource(resolvedSchema, getSourceConfig(config), parallelism);
    }

    private RocketMQConfig getSourceConfig(ReadableConfig config) {
        String topic = config.get(TOPIC);
        String consumerGroup = config.get(CONSUMER_GROUP);
        String nameServerAddress = config.get(NAME_SERVER_ADDRESS);
        String tag = config.get(OPTIONAL_TAG);
        String mode = config.get(OPTIONAL_START_OFFSET_MODE);
        String encoding = config.get(OPTIONAL_ENCODING);
        String accessKey = config.get(OPTIONAL_ACCESS_KEY);
        String secretKey = config.get(OPTIONAL_SECRET_KEY);
        String accessChannel = config.get(OPTIONAL_ACCESS_CHANNEL);
        int heartbeat = config.get(OPTIONAL_HEART_BEAT_BROKER_INTERVAL);
        int persistInterval = config.get(OPTIONAL_PERSIST_CONSUMER_INTERVAL);
        int batchSize = config.get(OPTIONAL_CONSUMER_BATCH_SIZE);
        long startMessageOffset = config.get(OPTIONAL_START_MESSAGE_OFFSET);
        long startMessageTimeStamp = config.get(OPTIONAL_START_MESSAGE_TIMESTAMP);

        String startDateTime = config.get(OPTIONAL_START_TIME);
        String timeZone = config.get(OPTIONAL_TIME_ZONE);
        long startTime = config.get(OPTIONAL_START_TIME_MILLS);
        if (startTime == -1) {
            if (!StringUtils.isNullOrWhitespaceOnly(startDateTime)) {
                try {
                    startTime = parseDateString(startDateTime, timeZone);
                } catch (ParseException e) {
                    throw new RuntimeException(
                            String.format(
                                    "Incorrect datetime format: %s, pls use ISO-8601 "
                                            + "complete date plus hours, minutes and seconds format:%s.",
                                    startDateTime, DATE_FORMAT),
                            e);
                }
            }
        }
        long stopInMs = Long.MAX_VALUE;
        String endDateTime = config.get(OPTIONAL_END_TIME);
        if (!StringUtils.isNullOrWhitespaceOnly(endDateTime)) {
            try {
                stopInMs = parseDateString(endDateTime, timeZone);
            } catch (ParseException e) {
                throw new RuntimeException(
                        String.format(
                                "Incorrect datetime format: %s, pls use ISO-8601 "
                                        + "complete date plus hours, minutes and seconds format:%s.",
                                endDateTime, DATE_FORMAT),
                        e);
            }
            Preconditions.checkArgument(
                    stopInMs >= startTime, "Start time should be less than stop time.");
        }

        return RocketMQConfig.builder()
                .startTimeMs(startMessageOffset < 0 ? startTime : -1L)
                .persistConsumerOffsetInterval(persistInterval)
                .startMessageTimeStamp(startMessageTimeStamp)
                .mode(RocketMQConfig.StartMode.getFromName(mode))
                .startMessageOffset(startMessageOffset)
                .heartbeatBrokerInterval(heartbeat)
                .nameserverAddress(nameServerAddress)
                .consumerGroup(consumerGroup)
                .accessChannel(accessChannel)
                .accessKey(accessKey)
                .secretKey(secretKey)
                .fetchSize(batchSize)
                .encoding(encoding)
                .endTimeMs(stopInMs)
                .topic(topic)
                .tag(tag)
                .build();
    }

    private void transformContext(DynamicTableFactory factory, Context context) {
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();
        Map<String, String> convertedOptions =
                normalizeOptionCaseAsFactory(factory, catalogOptions);
        catalogOptions.clear();
        for (Map.Entry<String, String> entry : convertedOptions.entrySet()) {
            catalogOptions.put(entry.getKey(), entry.getValue());
        }
    }

    private Map<String, String> normalizeOptionCaseAsFactory(
            Factory factory, Map<String, String> options) {
        Map<String, String> normalizedOptions = new HashMap<>(16);
        Map<String, String> requiredOptionKeysLowerCaseToOriginal =
                factory.requiredOptions().stream()
                        .collect(
                                Collectors.toMap(
                                        option -> option.key().toLowerCase(), ConfigOption::key));
        Map<String, String> optionalOptionKeysLowerCaseToOriginal =
                factory.optionalOptions().stream()
                        .collect(
                                Collectors.toMap(
                                        option -> option.key().toLowerCase(), ConfigOption::key));
        for (Map.Entry<String, String> entry : options.entrySet()) {
            final String catalogOptionKey = entry.getKey();
            final String catalogOptionValue = entry.getValue();
            normalizedOptions.put(
                    requiredOptionKeysLowerCaseToOriginal.containsKey(
                                    catalogOptionKey.toLowerCase())
                            ? requiredOptionKeysLowerCaseToOriginal.get(
                                    catalogOptionKey.toLowerCase())
                            : optionalOptionKeysLowerCaseToOriginal.getOrDefault(
                                    catalogOptionKey.toLowerCase(), catalogOptionKey),
                    catalogOptionValue);
        }
        return normalizedOptions;
    }

    private Long parseDateString(String dateString, String timeZone) throws ParseException {
        FastDateFormat simpleDateFormat =
                FastDateFormat.getInstance(DATE_FORMAT, TimeZone.getTimeZone(timeZone));
        return simpleDateFormat.parse(dateString).getTime();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(TOPIC);
        requiredOptions.add(CONSUMER_GROUP);
        requiredOptions.add(NAME_SERVER_ADDRESS);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(OPTIONAL_TAG);
        optionalOptions.add(OPTIONAL_ACCESS_KEY);
        optionalOptions.add(OPTIONAL_SECRET_KEY);
        optionalOptions.add(OPTIONAL_ACCESS_CHANNEL);
        optionalOptions.add(OPTIONAL_HEART_BEAT_BROKER_INTERVAL);
        optionalOptions.add(OPTIONAL_PERSIST_CONSUMER_INTERVAL);
        optionalOptions.add(OPTIONAL_CONSUMER_BATCH_SIZE);
        optionalOptions.add(OPTIONAL_START_OFFSET_MODE);
        optionalOptions.add(OPTIONAL_START_MESSAGE_TIMESTAMP);
        optionalOptions.add(OPTIONAL_START_MESSAGE_OFFSET);
        optionalOptions.add(OPTIONAL_START_TIME_MILLS);
        optionalOptions.add(OPTIONAL_START_TIME);
        optionalOptions.add(OPTIONAL_END_TIME);
        optionalOptions.add(OPTIONAL_TIME_ZONE);
        optionalOptions.add(OPTIONAL_ENCODING);
        optionalOptions.add(SCAN_PARALLELISM);
        return optionalOptions;
    }
}
