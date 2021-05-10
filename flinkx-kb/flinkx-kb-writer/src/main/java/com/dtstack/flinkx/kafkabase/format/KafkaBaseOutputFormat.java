/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.dtstack.flinkx.kafkabase.format;

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.exception.DataSourceException;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.kafkabase.writer.HeartBeatController;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.dtstack.flinkx.kafkabase.KafkaConfigKeys.KEY_ASSIGNER_DEFAULT_RULE;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaBaseOutputFormat extends BaseRichOutputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaBaseOutputFormat.class);
    //producer.close最长等待时间
    protected static final long CLOSE_TIME = 20000L;

    protected Properties props = new Properties();
    protected String timezone;
    protected String topic;
    protected Map<String, String> producerSettings;
    protected List<String> tableFields;
    //用户指定kafka分区字段
    protected List<String> partitionAssignColumns;
    //是否保证强制有序
    protected boolean dataCompelOrder;
    protected Set<String> keySet;
    protected static JsonDecoder jsonDecoder = new JsonDecoder();
    //和kafkaBroker连通性控制器
    protected HeartBeatController heartBeatController;

    @Override
    public void configure(Configuration parameters) {
        if(producerSettings != null && producerSettings.containsKey("bootstrap.servers")){
            String brokerList = producerSettings.get("bootstrap.servers");
            LOG.info("brokerList->{}",brokerList);
            String broker = brokerList.split(ConstantValue.COMMA_SYMBOL)[0];
            String[] split = broker.split(ConstantValue.COLON_SYMBOL);

            try {
                TelnetUtil.telnet(split[0], Integer.parseInt(split[1]));
            }catch (Exception e){
                throw new RuntimeException("telnet error, brokerList = " + brokerList);
            }
        }
        keySet = new TreeSet<>();
        keySet.addAll(KEY_ASSIGNER_DEFAULT_RULE);
        if (CollectionUtils.isNotEmpty(partitionAssignColumns)) {
            keySet.addAll(partitionAssignColumns);
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {}

    @Override
    @SuppressWarnings("unchecked")
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        try {
            Map<String, Object> map;
            int arity = row.getArity();
            if (tableFields != null && tableFields.size() >= arity) {
                map = new LinkedHashMap<>((arity << 2) / 3);
                for (int i = 0; i < arity; i++) {
                    map.put(tableFields.get(i), org.apache.flink.util.StringUtils.arrayAwareToString(row.getField(i)));
                }
            } else {
                if(arity == 1){
                    Object obj = row.getField(0);
                    if (obj instanceof Map) {
                        map = (Map<String, Object>) obj;
                    } else if (obj instanceof String) {
                        map = jsonDecoder.decode(obj.toString());
                    } else {
                        map = Collections.singletonMap("message", row.toString());
                    }
                }else{
                    map = Collections.singletonMap("message", row.toString());
                }
            }
            emit(map);

        } catch (Throwable e) {
            String errorMessage = ExceptionUtil.getErrorMessage(e);
            LOG.error("kafka writeSingleRecordInternal error:{}", errorMessage);
          //如果是数据源错误 直接抛出异常，而不是封装为WriteRecordException
            // 否则WriteRecordException会被上层捕获，导致任务无法结束
            if(e instanceof DataSourceException){
                throw (DataSourceException)e;
            }
            throw new WriteRecordException(errorMessage, e);
        }
    }

    /**
     * 根据默认的字段和指定的字段生成key
     * @param event
     * @return
     */
    public String generateKey(Map event) {
        List<String> values = new ArrayList<>(keySet.size());
        //1.数据源为mysqlbinlog或者oraclelogminer数据未拍平 2.event.size == 1 && event.containsKey("message")
        if(event.size() == 1 && event.containsKey("message")){
            Map<String, Object> map;
            Object obj = event.get("message");
            if (obj instanceof Map){
                map = (Map<String, Object>) obj;
                keySet.forEach(rule -> {
                    values.add(map.getOrDefault(rule, "").toString());
                });
            }else{
                //单条数据，并且partitionKey为message的特殊情况
                keySet.forEach(rule -> {
                    values.add(event.getOrDefault(rule, "").toString());
                });
            }
        }else{
            keySet.forEach(rule -> {
                values.add(event.getOrDefault(rule, "").toString());
            });
        }
        List<String> collect = values.stream()
                .filter(value -> StringUtils.isNotEmpty(value))
                .collect(Collectors.toList());
        return StringUtils.join(collect.toArray(), "-");
    }

    protected void emit(Map event) throws IOException {
        throw new RuntimeException("KafkaBaseOutputFormat.emit() should be override by subclass!");
    }

    @Override
    public void closeInternal() throws IOException {
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        notSupportBatchWrite("KafkaWriter");
    }

    @Override
    protected boolean isStreamButNoWriteCheckpoint() {
        return true;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setProducerSettings(Map<String, String> producerSettings) {
        this.producerSettings = producerSettings;
    }

    public void setRestoreConfig(RestoreConfig restoreConfig) {
        this.restoreConfig = restoreConfig;
    }

    public void setTableFields(List<String> tableFields) {
        this.tableFields = tableFields;
    }

    public void setHeartBeatController(HeartBeatController heartBeatController) {
        this.heartBeatController = heartBeatController;
    }

    public List<String> getPartitionAssignColumns() {
        return partitionAssignColumns;
    }

    public void setPartitionAssignColumns(List<String> partitionAssignColumns) {
        this.partitionAssignColumns = partitionAssignColumns;
    }

    public boolean isDataCompelOrder() {
        return dataCompelOrder;
    }

    public void setDataCompelOrder(boolean dataCompelOrder) {
        this.dataCompelOrder = dataCompelOrder;
    }
}
