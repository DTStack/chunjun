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

package com.dtstack.chunjun.connector.nebula.client;

import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.row.NebulaTableRow;
import com.dtstack.chunjun.connector.nebula.splitters.NebulaInputSplitter;
import com.dtstack.chunjun.connector.nebula.utils.GraphUtil;

import com.vesoft.nebula.client.graph.data.SSLParam;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.meta.MetaCache;
import com.vesoft.nebula.client.meta.MetaManager;
import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.scan.ScanResultIterator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.List;

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.RANK;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.VID;

@Slf4j
public class NebulaStorageClient implements Serializable {

    private static final long serialVersionUID = -5627797931752187660L;

    private final NebulaConfig nebulaConfig;

    private StorageClient client;

    private MetaManager metaManager;

    public NebulaStorageClient(NebulaConfig nebulaConfig) {
        this.nebulaConfig = nebulaConfig;
    }

    public void init() throws IOException {
        try {
            initMetaManagerAndClient();
            client.connect();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IOException(e);
        }
    }

    private void initMetaManagerAndClient()
            throws ClientServerIncompatibleException, UnknownHostException {
        boolean enableSSL = nebulaConfig.getEnableSSL();
        SSLParam sslParam = GraphUtil.getSslParam(nebulaConfig);
        metaManager =
                new MetaManager(
                        nebulaConfig.getStorageAddresses(),
                        nebulaConfig.getTimeout(),
                        nebulaConfig.getConnectionRetry(),
                        nebulaConfig.getExecutionRetry(),
                        enableSSL,
                        sslParam);
        client =
                new StorageClient(
                        nebulaConfig.getStorageAddresses(),
                        nebulaConfig.getTimeout(),
                        nebulaConfig.getConnectionRetry(),
                        nebulaConfig.getExecutionRetry(),
                        enableSSL,
                        sslParam);
    }

    /** fetch data from nebula */
    public NebulaTableRow fetchRangeData(
            Integer part, Long scanStart, NebulaInputSplitter inputSplitter) {
        ScanResultIterator scanResultIterator = null;
        List<String> columnNames = nebulaConfig.getColumnNames();
        long ScanEnd =
                (scanStart + inputSplitter.getInterval()) < inputSplitter.getScanEnd()
                        ? scanStart + inputSplitter.getInterval()
                        : inputSplitter.getScanEnd();
        //        log.debug("part is {},scanStart is {}, ScanEnd is {} ", part, scanStart, ScanEnd);
        switch (nebulaConfig.getSchemaType()) {
            case VERTEX:
            case TAG:
                List<String> var1 = columnNames.subList(1, columnNames.size());
                scanResultIterator =
                        client.scanVertex(
                                nebulaConfig.getSpace(),
                                part,
                                nebulaConfig.getEntityName(),
                                var1,
                                nebulaConfig.getFetchSize(),
                                scanStart,
                                (scanStart + inputSplitter.getInterval())
                                                < inputSplitter.getScanEnd()
                                        ? scanStart + inputSplitter.getInterval()
                                        : inputSplitter.getScanEnd(),
                                nebulaConfig.getDefaultAllowPartSuccess(),
                                nebulaConfig.getDefaultAllowReadFollower());
                break;
            case EDGE:
            case EDGE_TYPE:
                List<String> var2 =
                        columnNames.contains(RANK)
                                ? columnNames.subList(3, columnNames.size())
                                : columnNames.subList(2, columnNames.size());
                scanResultIterator =
                        client.scanEdge(
                                nebulaConfig.getSpace(),
                                part,
                                nebulaConfig.getEntityName(),
                                var2,
                                nebulaConfig.getFetchSize(),
                                scanStart,
                                (scanStart + inputSplitter.getInterval())
                                                < inputSplitter.getScanEnd()
                                        ? scanStart + inputSplitter.getInterval()
                                        : inputSplitter.getScanEnd(),
                                nebulaConfig.getDefaultAllowPartSuccess(),
                                nebulaConfig.getDefaultAllowReadFollower());
                break;
        }
        return new NebulaTableRow(scanResultIterator, nebulaConfig);
    }

    /** fetch data from nebula */
    public NebulaTableRow fetchAllData() {
        ScanResultIterator scanResultIterator = null;
        List<String> columnNames = nebulaConfig.getColumnNames();
        switch (nebulaConfig.getSchemaType()) {
            case VERTEX:
            case TAG:
                List<String> var1 = columnNames.subList(1, columnNames.size());
                columnNames.remove(VID);
                scanResultIterator =
                        client.scanVertex(
                                nebulaConfig.getSpace(),
                                nebulaConfig.getEntityName(),
                                var1,
                                nebulaConfig.getFetchSize(),
                                Long.MIN_VALUE,
                                Long.MAX_VALUE,
                                nebulaConfig.getDefaultAllowPartSuccess(),
                                nebulaConfig.getDefaultAllowReadFollower());
                break;
            case EDGE:
            case EDGE_TYPE:
                List<String> var2 =
                        columnNames.contains(RANK)
                                ? columnNames.subList(3, columnNames.size())
                                : columnNames.subList(2, columnNames.size());
                scanResultIterator =
                        client.scanEdge(
                                nebulaConfig.getSpace(),
                                nebulaConfig.getEntityName(),
                                var2,
                                nebulaConfig.getFetchSize(),
                                Long.MIN_VALUE,
                                Long.MAX_VALUE,
                                nebulaConfig.getDefaultAllowPartSuccess(),
                                nebulaConfig.getDefaultAllowReadFollower());
                break;
        }
        return new NebulaTableRow(scanResultIterator, nebulaConfig);
    }

    public Boolean isSchemaExist(String space, String schemaName) {
        switch (nebulaConfig.getSchemaType()) {
            case VERTEX:
            case TAG:
                try {
                    metaManager.getTag(space, schemaName);
                    return true;
                } catch (Exception e) {
                    return false;
                }
            case EDGE:
            case EDGE_TYPE:
                try {
                    metaManager.getEdge(space, schemaName);
                    return true;
                } catch (Exception e) {
                    return false;
                }
        }
        return false;
    }

    public Boolean isSpaceExist(String space) {
        try {
            metaManager.getSpace(space);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void close() {
        if (metaManager != null) {
            metaManager.close();
        }
        if (client != null) {
            client.close();
        }
    }

    public MetaCache getMetaManager() throws IOException {
        if (metaManager == null) {
            init();
        }
        return metaManager;
    }
}
