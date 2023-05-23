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

package com.dtstack.chunjun.connector.oraclelogminer.inputformat;

import com.dtstack.chunjun.connector.oraclelogminer.config.LogMinerConfig;
import com.dtstack.chunjun.connector.oraclelogminer.listener.LogMinerListener;
import com.dtstack.chunjun.connector.oraclelogminer.listener.PositionManager;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.math.BigInteger;

public class OracleLogMinerInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = -5365632049708957327L;

    public LogMinerConfig logMinerConfig;

    private transient LogMinerListener logMinerListener;

    private transient PositionManager positionManager;

    private AbstractCDCRawTypeMapper rowConverter;

    @Override
    protected InputSplit[] createInputSplitsInternal(int i) {
        return new InputSplit[] {new GenericInputSplit(1, 1)};
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        positionManager = new PositionManager();
        initPosition();

        logMinerListener = new LogMinerListener(logMinerConfig, positionManager, rowConverter);
    }

    private void initPosition() {
        if (null != formatState && formatState.getState() != null) {
            BigInteger position = new BigInteger(formatState.getState().toString());
            // 查询数据时时左闭右开区间 所以需要将上次消费位点+1
            position = position.add(BigInteger.ONE);
            positionManager.setScn(position);
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        logMinerListener.init();
        logMinerListener.start();
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();

        if (formatState != null) {
            formatState.setState(positionManager.getScn());
        }

        return formatState;
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) {
        return logMinerListener.getData();
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (null != logMinerListener) {
            try {
                logMinerListener.stop();
            } catch (Exception e) {
                throw new IOException("close listener error", e);
            }
        }
    }

    public void setRowConverter(AbstractCDCRawTypeMapper rowConverter) {
        this.rowConverter = rowConverter;
    }
}
