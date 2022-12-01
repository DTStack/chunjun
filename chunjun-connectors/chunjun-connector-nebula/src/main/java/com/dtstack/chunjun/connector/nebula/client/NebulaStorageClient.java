package com.dtstack.chunjun.connector.nebula.client;
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

import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.connector.nebula.row.NebulaTableRow;
import com.dtstack.chunjun.connector.nebula.splitters.NebulaInputSplitter;
import com.dtstack.chunjun.connector.nebula.utils.GraphUtil;

import com.vesoft.nebula.client.graph.data.SSLParam;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.meta.MetaCache;
import com.vesoft.nebula.client.meta.MetaManager;
import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.scan.ScanResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.List;

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.RANK;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.VID;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/11/10 3:45 下午
 */
public class NebulaStorageClient implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(NebulaStorageClient.class);

    private NebulaConf nebulaConf;

    private StorageClient client;

    private MetaManager metaManager;

    public NebulaStorageClient(NebulaConf nebulaConf) {
        this.nebulaConf = nebulaConf;
    }

    public void init() throws IOException {
        try {
            initMetaManagerAndClient();
            client.connect();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new IOException(e);
        }
    }

    private void initMetaManagerAndClient()
            throws ClientServerIncompatibleException, UnknownHostException {
        boolean enableSSL = nebulaConf.getEnableSSL();
        SSLParam sslParam = GraphUtil.getSslParam(nebulaConf);
        metaManager =
                new MetaManager(
                        nebulaConf.getStorageAddresses(),
                        nebulaConf.getTimeout(),
                        nebulaConf.getConnectionRetry(),
                        nebulaConf.getExecutionRetry(),
                        enableSSL,
                        sslParam);
        client =
                new StorageClient(
                        nebulaConf.getStorageAddresses(),
                        nebulaConf.getTimeout(),
                        nebulaConf.getConnectionRetry(),
                        nebulaConf.getExecutionRetry(),
                        enableSSL,
                        sslParam);
    }

    /**
     * fetch data from nebula
     *
     * @throws IOErrorException
     */
    public NebulaTableRow fetchRangeData(
            Integer part, Long scanStart, NebulaInputSplitter inputSplitter) throws Exception {
        ScanResultIterator scanResultIterator = null;
        List<String> columnNames = nebulaConf.getColumnNames();
        long ScanEnd =
                (scanStart + inputSplitter.getInterval()) < inputSplitter.getScanEnd()
                        ? scanStart + inputSplitter.getInterval()
                        : inputSplitter.getScanEnd();
        //        LOG.debug("part is {},scanStart is {}, ScanEnd is {} ", part, scanStart, ScanEnd);
        switch (nebulaConf.getSchemaType()) {
            case VERTEX:
            case TAG:
                List<String> var1 = columnNames.subList(1, columnNames.size());
                scanResultIterator =
                        client.scanVertex(
                                nebulaConf.getSpace(),
                                part,
                                nebulaConf.getEntityName(),
                                var1,
                                nebulaConf.getFetchSize(),
                                scanStart,
                                (scanStart + inputSplitter.getInterval())
                                                < inputSplitter.getScanEnd()
                                        ? scanStart + inputSplitter.getInterval()
                                        : inputSplitter.getScanEnd(),
                                nebulaConf.getDefaultAllowPartSuccess(),
                                nebulaConf.getDefaultAllowReadFollower());
                break;
            case EDGE:
            case EDGE_TYPE:
                List<String> var2 =
                        columnNames.contains(RANK)
                                ? columnNames.subList(3, columnNames.size())
                                : columnNames.subList(2, columnNames.size());
                scanResultIterator =
                        client.scanEdge(
                                nebulaConf.getSpace(),
                                part,
                                nebulaConf.getEntityName(),
                                var2,
                                nebulaConf.getFetchSize(),
                                scanStart,
                                (scanStart + inputSplitter.getInterval())
                                                < inputSplitter.getScanEnd()
                                        ? scanStart + inputSplitter.getInterval()
                                        : inputSplitter.getScanEnd(),
                                nebulaConf.getDefaultAllowPartSuccess(),
                                nebulaConf.getDefaultAllowReadFollower());
                break;
        }
        return new NebulaTableRow(scanResultIterator, nebulaConf);
    }

    /**
     * fetch data from nebula
     *
     * @throws IOErrorException
     */
    public NebulaTableRow fetchAllData() throws Exception {
        ScanResultIterator scanResultIterator = null;
        List<String> columnNames = nebulaConf.getColumnNames();
        switch (nebulaConf.getSchemaType()) {
            case VERTEX:
            case TAG:
                List<String> var1 = columnNames.subList(1, columnNames.size());
                columnNames.remove(VID);
                scanResultIterator =
                        client.scanVertex(
                                nebulaConf.getSpace(),
                                nebulaConf.getEntityName(),
                                var1,
                                nebulaConf.getFetchSize(),
                                Long.MIN_VALUE,
                                Long.MAX_VALUE,
                                nebulaConf.getDefaultAllowPartSuccess(),
                                nebulaConf.getDefaultAllowReadFollower());
                break;
            case EDGE:
            case EDGE_TYPE:
                List<String> var2 =
                        columnNames.contains(RANK)
                                ? columnNames.subList(3, columnNames.size())
                                : columnNames.subList(2, columnNames.size());
                scanResultIterator =
                        client.scanEdge(
                                nebulaConf.getSpace(),
                                nebulaConf.getEntityName(),
                                var2,
                                nebulaConf.getFetchSize(),
                                Long.MIN_VALUE,
                                Long.MAX_VALUE,
                                nebulaConf.getDefaultAllowPartSuccess(),
                                nebulaConf.getDefaultAllowReadFollower());
                break;
        }
        return new NebulaTableRow(scanResultIterator, nebulaConf);
    }

    public Boolean isSchemaExist(String space, String schemaName) {
        switch (nebulaConf.getSchemaType()) {
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
