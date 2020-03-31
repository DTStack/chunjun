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


package com.dtstack.flinkx.oraclelogminer.format;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.restore.FormatState;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @author jiangbo
 * @date 2019/12/14
 *
 * 名词说明:
 * SCN 即系统改变号(System Change Number)
 */
public class OracleLogMinerInputFormat extends RichInputFormat {

    public LogMinerConfig logMinerConfig;

    private transient LogMinerListener logMinerListener;

    private transient PositionManager positionManager;

    @Override
    protected InputSplit[] createInputSplitsInternal(int i) throws Exception {
        return new InputSplit[]{new GenericInputSplit(1,1)};
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        positionManager = new PositionManager();
        initPosition();

        logMinerListener = new LogMinerListener(logMinerConfig, positionManager);
    }

    private void initPosition() {
        if (null != formatState && formatState.getState() != null) {
            positionManager.updatePosition((Long)formatState.getState());
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        logMinerListener.init();
        logMinerListener.start();
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return Row.of(logMinerListener.getData());
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();

        if (formatState != null) {
            formatState.setState(positionManager.getPosition());
        }

        return formatState;
    }

    @Override
    public boolean reachedEnd() throws IOException {
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

    enum ReadPosition{
        ALL, CURRENT, TIME, SCN
    }
}
