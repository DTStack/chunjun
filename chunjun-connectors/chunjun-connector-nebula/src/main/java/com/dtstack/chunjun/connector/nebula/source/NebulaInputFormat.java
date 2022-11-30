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

package com.dtstack.chunjun.connector.nebula.source;
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
import com.dtstack.chunjun.connector.nebula.client.NebulaClientFactory;
import com.dtstack.chunjun.connector.nebula.client.NebulaStorageClient;
import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.connector.nebula.row.NebulaTableRow;
import com.dtstack.chunjun.connector.nebula.splitters.NebulaInputSplitter;
import com.dtstack.chunjun.connector.nebula.splitters.creator.BaseSplitResponsibility;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: gaoasi
 * @create: 2022/09/22
 */
public class NebulaInputFormat extends BaseRichInputFormat {
    private final Logger LOG = LoggerFactory.getLogger(NebulaInputFormat.class);
    public static final long serialVersionUID = 1L;
    protected NebulaConf nebulaConf;
    protected NebulaTableRow nebulaTableRow;
    protected NebulaInputSplitter currentInputSplit;
    private NebulaStorageClient storageClient;
    protected Long scanStart;

    protected Object state;

    protected Boolean hasNext;

    protected Integer currentNebulaPart;

    public NebulaConf getNebulaConf() {
        return nebulaConf;
    }

    public void setNebulaConf(NebulaConf nebulaConf) {
        this.nebulaConf = nebulaConf;
        storageClient = NebulaClientFactory.createNebulaStorageClient(nebulaConf);
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        storageClient.init();
        LOG.debug("inited nebula storage client!");
        List<Integer> spaceParts =
                new ArrayList<>(
                        storageClient
                                .getMetaManager()
                                .getPartsAlloc(nebulaConf.getSpace())
                                .keySet());
        LOG.debug("space parts collections is " + spaceParts);
        NebulaInputSplitter[] inputSplits = new NebulaInputSplitter[minNumSplits];
        BaseSplitResponsibility baseSplitResponsibility = new BaseSplitResponsibility(true);
        baseSplitResponsibility.createSplit(
                minNumSplits, spaceParts.size(), inputSplits, spaceParts, nebulaConf);

        LOG.debug("inputSplits are " + inputSplits);
        closeInternal();
        return inputSplits;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        currentInputSplit = (NebulaInputSplitter) inputSplit;

        if (storageClient.getMetaManager() == null) storageClient.init();
        scanStart = currentInputSplit.getScanStart();
        try {
            if (state != null) scanStart = (Long) state;
            else state = scanStart;
            currentInputSplit.setScanStart(scanStart);
            currentNebulaPart = currentInputSplit.parts.poll();
            nebulaTableRow =
                    storageClient.fetchRangeData(currentNebulaPart, scanStart, currentInputSplit);
            hasNext = nebulaTableRow.hasNext();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        if (!hasNext) {
            return null;
        }
        try {
            rowData = rowConverter.toInternal(nebulaTableRow.next());
            return rowData;
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        } finally {
            hasNext = nebulaTableRow.hasNext();
        }
    }

    @Override
    protected void closeInternal() {
        if (storageClient != null) {
            storageClient.close();
        }
    }

    @Override
    public boolean reachedEnd() {
        if (!hasNext) {
            if (scanStart + currentInputSplit.getInterval() < currentInputSplit.getScanEnd()) {
                scanStart += currentInputSplit.getInterval();
            } else if (!currentInputSplit.parts.isEmpty()) {
                scanStart = currentInputSplit.getScanStart();
                currentNebulaPart = currentInputSplit.parts.poll();
            } else {
                return true;
            }
            try {
                nebulaTableRow =
                        storageClient.fetchRangeData(
                                currentNebulaPart, scanStart, currentInputSplit);
                hasNext = nebulaTableRow.hasNext();
            } catch (Exception e) {
                throw new ChunJunRuntimeException(e.getMessage(), e);
            }
            state = scanStart;
        }
        return !hasNext;
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        formatState.setState(state);
        return formatState;
    }
}
