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

package com.dtstack.chunjun.connector.starrocks.source.be;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.ColumnInfo;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.starrocks.shade.org.apache.thrift.TException;
import com.starrocks.shade.org.apache.thrift.protocol.TBinaryProtocol;
import com.starrocks.shade.org.apache.thrift.protocol.TProtocol;
import com.starrocks.shade.org.apache.thrift.transport.TSocket;
import com.starrocks.shade.org.apache.thrift.transport.TTransportException;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanColumnDesc;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TStarrocksExternalService;
import com.starrocks.thrift.TStatusCode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class StarRocksSourceBeReader {

    private final StarRocksConfig starRocksConfig;
    private final String beHost;

    private final TStarrocksExternalService.Client client;
    private String contextId;
    private int readerOffset = 0;

    private StarRocksArrowReader curArrowReader;
    private Object[] curData;
    private List<ColumnInfo> columnInfoList;

    public StarRocksSourceBeReader(String beNode, StarRocksConfig starRocksConfig) {
        this.starRocksConfig = starRocksConfig;
        String[] beNodeInfo = beNode.split(":");
        this.beHost = beNodeInfo[0].trim();
        int bePort = Integer.parseInt(beNodeInfo[1].trim());
        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSocket socket =
                new TSocket(
                        beHost,
                        bePort,
                        starRocksConfig.getBeClientTimeout(),
                        starRocksConfig.getBeClientTimeout());
        try {
            socket.open();
        } catch (TTransportException e) {
            socket.close();
            throw new RuntimeException("Failed to create brpc source:" + e.getMessage());
        }
        TProtocol protocol = factory.getProtocol(socket);
        client = new TStarrocksExternalService.Client(protocol);
    }

    public void openScanner(List<Long> tablets, String opaqued_query_plan) {
        TScanOpenParams params = new TScanOpenParams();
        params.setTablet_ids(tablets);
        params.setOpaqued_query_plan(opaqued_query_plan);
        params.setCluster("default_cluster");
        params.setDatabase(starRocksConfig.getDatabase());
        params.setTable(starRocksConfig.getTable());
        params.setUser(starRocksConfig.getUsername());
        params.setPasswd(starRocksConfig.getPassword());
        params.setBatch_size(starRocksConfig.getBeFetchRows());
        params.setMem_limit(starRocksConfig.getBeFetchMaxBytes());
        params.setProperties(starRocksConfig.getBeSocketProperties());
        params.setKeep_alive_min((short) starRocksConfig.getBeClientKeepLiveMin());
        params.setQuery_timeout(starRocksConfig.getBeQueryTimeoutSecond());
        log.info("open Scan params.mem_limit {} B", params.getMem_limit());
        log.info("open Scan params.keep-alive-min {} min", params.getKeep_alive_min());
        TScanOpenResult result;
        int times = 0;
        while (true) {
            try {
                result = client.open_scanner(params);
                if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                    throw new RuntimeException(
                            "Failed to open scanner."
                                    + result.getStatus().getStatus_code()
                                    + result.getStatus().getError_msgs());
                }
                break;
            } catch (TException e) {
                if (++times <= starRocksConfig.getMaxRetries()) {
                    log.info(
                            String.format("failed to open beScanner,current retryTimes:%s", times));
                } else {
                    throw new ChunJunRuntimeException(
                            String.format("failed to scan from be,scanParams[%s]", params), e);
                }
            }
        }
        this.contextId = result.getContext_id();
        initNameTypeMap(result.selected_columns);
    }

    private void initNameTypeMap(List<TScanColumnDesc> selectedColumnList) {
        columnInfoList = new ArrayList<>(selectedColumnList.size());
        String[] fieldNames = starRocksConfig.getFieldNames();
        DataType[] dataTypes = starRocksConfig.getDataTypes();
        if (selectedColumnList.size() != fieldNames.length) {
            throw new ChunJunRuntimeException(
                    "be selected column size does not match configuration column size");
        }

        for (int i = 0; i < selectedColumnList.size(); i++) {
            String fieldName = fieldNames[i];
            LogicalTypeRoot logicalTypeRoot = dataTypes[i].getLogicalType().getTypeRoot();
            String starRocksType = null;
            for (TScanColumnDesc columnDesc : selectedColumnList) {
                if (fieldName.equalsIgnoreCase(columnDesc.getName())) {
                    starRocksType = columnDesc.getType().name();
                }
            }
            if (starRocksType == null) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "be selected column does not contain column[%s],please check your configuration",
                                fieldName));
            }
            columnInfoList.add(new ColumnInfo(fieldName, logicalTypeRoot, starRocksType, i));
        }
    }

    public void startToRead() {
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(this.contextId);
        params.setOffset(this.readerOffset);
        TScanBatchResult result;
        int times = 0;
        while (true) {
            try {
                result = client.get_next(params);
                if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                    throw new RuntimeException(
                            "Failed to get next from be -> ip:["
                                    + beHost
                                    + "] "
                                    + result.getStatus().getStatus_code()
                                    + " msg:"
                                    + result.getStatus().getError_msgs());
                }
                break;
            } catch (Exception e) {
                if (++times <= starRocksConfig.getMaxRetries()) {
                    log.info(String.format("failed to scan from be,current retryTimes:%s", times));
                } else {
                    throw new ChunJunRuntimeException(
                            String.format("failed to scan from be,scanParams[%s]", params), e);
                }
            }
        }
        if (!result.eos) {
            handleResult(result);
        }
    }

    public boolean hasNext() {
        return this.curData != null;
    }

    public Object[] getNext() {
        Object[] preparedData = this.curData;
        this.curData = null;
        if (this.curArrowReader.hasNext()) {
            this.curData = curArrowReader.next();
        }
        if (this.curData != null) {
            return preparedData;
        }
        startToRead();
        return preparedData;
    }

    private void handleResult(TScanBatchResult result) {
        StarRocksArrowReader starRocksRowData;
        try {
            starRocksRowData = new StarRocksArrowReader(result, columnInfoList).read();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        this.readerOffset = starRocksRowData.getReadRowCount() + this.readerOffset;
        this.curArrowReader = starRocksRowData;
        this.curData = starRocksRowData.next();
    }

    public void close() {
        TScanCloseParams tScanCloseParams = new TScanCloseParams();
        tScanCloseParams.setContext_id(this.contextId);
        try {
            this.client.close_scanner(tScanCloseParams);
        } catch (TException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
