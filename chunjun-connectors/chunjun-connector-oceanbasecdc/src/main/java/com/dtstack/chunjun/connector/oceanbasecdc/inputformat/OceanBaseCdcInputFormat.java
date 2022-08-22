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

package com.dtstack.chunjun.connector.oceanbasecdc.inputformat;

import com.dtstack.chunjun.connector.oceanbasecdc.conf.OceanBaseCdcConf;
import com.dtstack.chunjun.connector.oceanbasecdc.listener.OceanBaseCdcListener;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("rawtypes")
public class OceanBaseCdcInputFormat extends BaseRichInputFormat {

    public transient LinkedBlockingDeque<RowData> queue;
    public volatile boolean running = false;
    public String safeTimestamp;

    private OceanBaseCdcConf cdcConf;
    private AbstractCDCRowConverter rowConverter;

    private transient Thread cdcListenerThread;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        if (inputSplit.getSplitNumber() != 0) {
            LOG.info("openInternal split number: {} abort...", inputSplit.getSplitNumber());
            return;
        }
        LOG.info("openInternal split number: {} start...", inputSplit.getSplitNumber());
        LOG.info("cdcConf: {}", JsonUtil.toPrintJson(cdcConf));

        queue = new LinkedBlockingDeque<>(1000);

        cdcListenerThread = new Thread(new OceanBaseCdcListener(this));
        cdcListenerThread.setName("cdcListenerThread");
        cdcListenerThread.start();
        running = true;

        LOG.info("OceanBaseCdcInputFormat[{}]open: end", jobName);
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null) {
            formatState.setState(safeTimestamp);
        }
        return formatState;
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            rowData = queue.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
            throw new ReadRecordException("takeEvent interrupted error", e);
        }
        return rowData;
    }

    @Override
    protected void closeInternal() {
        if (running) {
            running = false;
            if (cdcListenerThread != null) {
                cdcListenerThread.interrupt();
                cdcListenerThread = null;
            }
            LOG.warn("shutdown OceanBaseCdcInputFormat......");
        }
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    public OceanBaseCdcConf getCdcConf() {
        return cdcConf;
    }

    public void setCdcConf(OceanBaseCdcConf cdcConf) {
        this.cdcConf = cdcConf;
    }

    public AbstractCDCRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
