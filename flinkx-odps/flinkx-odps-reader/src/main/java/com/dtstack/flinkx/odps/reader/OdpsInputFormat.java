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

package com.dtstack.flinkx.odps.reader;

import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.odps.OdpsUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The odps implementation of InputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OdpsInputFormat extends BaseRichInputFormat {

    protected List<MetaColumn> metaColumns;

    protected String partition;

    protected String projectName;

    protected String tableName;

    protected boolean compress = false;

    protected Map<String,String> odpsConfig;

    protected String tunnelServer;

    protected long startIndex;

    protected long stepCount;

    private transient Odps odps;

    private transient TableTunnel.DownloadSession downloadSession;

    private transient RecordReader recordReader;

    private transient Record record;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        odps = OdpsUtil.initOdps(odpsConfig);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int adviceNum) throws IOException {
        Odps odps = OdpsUtil.initOdps(odpsConfig);
        TableTunnel.DownloadSession session;
        if(StringUtils.isNotBlank(partition)) {
            session = OdpsUtil.createMasterSessionForPartitionedTable(odps, tunnelServer, projectName, tableName, partition);
        } else {
            session = OdpsUtil.createMasterSessionForNonPartitionedTable(odps, tunnelServer, projectName, tableName);
        }

        return split(session, adviceNum);
    }

    private OdpsInputSplit[] split(final TableTunnel.DownloadSession session, int adviceNum) {
        List<OdpsInputSplit> splits = new ArrayList<OdpsInputSplit>();

        long count = session.getRecordCount();

        List<Pair<Long, Long>> splitResult = OdpsUtil.splitRecordCount(count, adviceNum);

        for (Pair<Long, Long> pair : splitResult) {
            long startIndex = pair.getLeft();
            long stepCount = pair.getRight();
            OdpsInputSplit split = new OdpsInputSplit(session.getId(), startIndex, stepCount);
            if(startIndex < stepCount) {
                splits.add(split);
            }
        }

        return splits.toArray(new OdpsInputSplit[splits.size()]);
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        OdpsInputSplit split = (OdpsInputSplit) inputSplit;
        String sessionId = split.getSessionId();
        startIndex = split.getStartIndex();
        stepCount = split.getStepCount();

        if(StringUtils.isNotBlank(partition)) {
            downloadSession = OdpsUtil.getSlaveSessionForPartitionedTable(odps, sessionId, tunnelServer, projectName, tableName, partition);
        } else {
            downloadSession = OdpsUtil.getSlaveSessionForNonPartitionedTable(odps, sessionId, tunnelServer, projectName, tableName);
        }

        recordReader = OdpsUtil.getRecordReader(downloadSession, startIndex, stepCount, compress);

    }

    @Override
    public boolean reachedEnd() throws IOException {
        record = recordReader.read();
        return record == null;
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (metaColumns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())){
            row = new Row(record.getColumnCount());
            for (int i = 0; i < record.getColumnCount(); i++) {
                row.setField(i,record.get(i));
            }
        } else {
            row = new Row(metaColumns.size());
            for (int i = 0; i < metaColumns.size(); i++) {
                MetaColumn metaColumn = metaColumns.get(i);

                Object val = null;
                if(metaColumn.getName() != null){
                    val = record.get(metaColumn.getName());

                    if(val == null && metaColumn.getValue() != null){
                        val = metaColumn.getValue();
                    }

                    if(val instanceof byte[]) {
                        val = new String((byte[]) val, StandardCharsets.UTF_8);
                    }
                } else if(metaColumn.getValue() != null){
                    val = metaColumn.getValue();
                }

                if(val != null && val instanceof String){
                    val = StringUtil.string2col(String.valueOf(val),metaColumn.getType(),metaColumn.getTimeFormat());
                }

                row.setField(i,val);
            }
        }

        return row;
    }

    @Override
    public void closeInternal() throws IOException {
        if (recordReader != null) {
            recordReader.close();
        }
    }

}
