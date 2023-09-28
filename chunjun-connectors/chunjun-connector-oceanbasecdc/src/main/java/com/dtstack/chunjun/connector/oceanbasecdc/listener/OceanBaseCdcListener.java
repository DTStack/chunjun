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

package com.dtstack.chunjun.connector.oceanbasecdc.listener;

import com.dtstack.chunjun.connector.oceanbasecdc.config.OceanBaseCdcConfig;
import com.dtstack.chunjun.connector.oceanbasecdc.entity.OceanBaseCdcEventRow;
import com.dtstack.chunjun.connector.oceanbasecdc.inputformat.OceanBaseCdcInputFormat;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.table.data.RowData;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "unchecked"})
@Slf4j
public class OceanBaseCdcListener implements Runnable {

    private final OceanBaseCdcInputFormat format;
    private final OceanBaseCdcConfig cdcConf;
    private final AbstractCDCRawTypeMapper rowConverter;
    private final List<DataMessage.Record.Type> categories;

    private final List<LogMessage> logMessageBuffer = new LinkedList<>();

    public OceanBaseCdcListener(OceanBaseCdcInputFormat format) {
        this.format = format;
        this.cdcConf = format.getCdcConf();
        this.rowConverter = format.getCdcRowConverter();
        this.categories =
                Arrays.stream(format.getCdcConf().getCat().split(ConstantValue.COMMA_SYMBOL))
                        .map(String::trim)
                        .map(String::toUpperCase)
                        .map(DataMessage.Record.Type::valueOf)
                        .collect(Collectors.toList());
    }

    @Override
    public void run() {
        log.info("OceanBaseCdcListener start running.....");

        ObReaderConfig obReaderConfig = cdcConf.getObReaderConfig();
        if (StringUtils.isNotBlank(format.safeTimestamp)) {
            obReaderConfig.updateCheckpoint(format.safeTimestamp);
        }

        LogProxyClient client =
                new LogProxyClient(
                        cdcConf.getLogProxyHost(), cdcConf.getLogProxyPort(), obReaderConfig);

        client.addListener(
                new RecordListener() {

                    @Override
                    public void notify(LogMessage message) {
                        if (!format.running) {
                            client.stop();
                            return;
                        }

                        if (log.isDebugEnabled()) {
                            log.debug(message.toString());
                        }

                        switch (message.getOpt()) {
                            case HEARTBEAT:
                            case BEGIN:
                                break;
                            case INSERT:
                            case UPDATE:
                            case DELETE:
                                logMessageBuffer.add(message);
                                break;
                            case COMMIT:
                                flushBuffer();
                                break;
                            case DDL:
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "Ddl: {}",
                                            message.getFieldList().get(0).getValue().toString());
                                }
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported type: " + message.getOpt());
                        }
                    }

                    @Override
                    public void onException(LogProxyClientException e) {
                        log.error("LogProxyClient exception", e);
                        client.stop();
                    }
                });

        client.start();
        client.join();
    }

    private void flushBuffer() {
        List<LogMessage> logMessages =
                logMessageBuffer.stream()
                        .filter(msg -> categories.contains(msg.getOpt()))
                        .collect(Collectors.toList());
        for (LogMessage logMessage : logMessages) {
            OceanBaseCdcEventRow eventRow = new OceanBaseCdcEventRow(logMessage);
            try {
                LinkedList<RowData> rowDatalist = rowConverter.toInternal(eventRow);
                RowData rowData;
                while ((rowData = rowDatalist.poll()) != null) {
                    format.queue.put(rowData);
                    format.safeTimestamp = logMessage.getSafeTimestamp();
                }
            } catch (Exception e) {
                throw new ChunJunRuntimeException("flush log message buffer failed", e);
            }
        }
        logMessageBuffer.clear();
    }
}
