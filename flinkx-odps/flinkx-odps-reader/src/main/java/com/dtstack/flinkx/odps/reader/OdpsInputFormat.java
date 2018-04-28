/**
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
import com.aliyun.odps.Table;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.odps.OdpsUtil;
import com.dtstack.flinkx.reader.ByteRateLimiter;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The odps implementation of InputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OdpsInputFormat extends RichInputFormat {

    protected List<String> columnName;

    protected List<String> columnValue;

    protected List<String> columnType;

    protected String sessionId;

    protected String partition;

    protected String projectName;

    protected String tableName;

    protected boolean compress = false;

    protected Map<String,String> odpsConfig;

    protected String tunnelServer;

    protected boolean isPartitioned = false;

    protected long startIndex;

    protected long stepCount;

    private Odps odps;

    private TableTunnel.DownloadSession downloadSession;

    private TableTunnel tunnel;

    private RecordReader recordReader;

    private Record record;

    private Table table;


    @Override
    public void configure(Configuration configuration) {
        odps = OdpsUtil.initOdps(odpsConfig);
        table = OdpsUtil.getTable(odps, projectName, tableName);
        isPartitioned = OdpsUtil.isPartitionedTable(table);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int adviceNum) throws IOException {
        TableTunnel.DownloadSession session = null;
        if(isPartitioned) {
            session = OdpsUtil.createMasterSessionForPartitionedTable(odps, tunnelServer, projectName, tableName, partition);
        } else {
            session = OdpsUtil.createMasterSessionForNonPartitionedTable(odps, tunnelServer, projectName, tableName);
        }

        if(session != null) {
            sessionId = session.getId();
        }

        return split(session, adviceNum);
    }

    private OdpsInputSplit[] split(final TableTunnel.DownloadSession session, int adviceNum) {
        List<OdpsInputSplit> splits = new ArrayList<OdpsInputSplit>();

        long count = session.getRecordCount();

        List<Pair<Long, Long>> splitResult = OdpsUtil.splitRecordCount(count, adviceNum);

        for (Pair<Long, Long> pair : splitResult) {
            long startIndex = pair.getLeft().longValue();
            long stepCount = pair.getRight().longValue();
            OdpsInputSplit split = new OdpsInputSplit(session.getId(), startIndex, stepCount);
            splits.add(split);
        }

        return splits.toArray(new OdpsInputSplit[splits.size()]);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        OdpsInputSplit split = (OdpsInputSplit) inputSplit;
        sessionId = split.getSessionId();
        startIndex = split.getStartIndex();
        stepCount = split.getStepCount();

        if(isPartitioned) {
            downloadSession = OdpsUtil.getSlaveSessionForPartitionedTable(odps, sessionId, tunnelServer, projectName, tableName, partition);
        } else {
            downloadSession = OdpsUtil.getSlaveSessionForNonPartitionedTable(odps, sessionId, tunnelServer, projectName, tableName);
        }

        recordReader = OdpsUtil.getRecordReader(downloadSession, startIndex, stepCount, compress);

        if(StringUtils.isNotBlank(monitorUrls) && this.bytes > 0) {
            this.byteRateLimiter = new ByteRateLimiter(getRuntimeContext(), monitorUrls, bytes, 1);
            this.byteRateLimiter.start();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        record = recordReader.read();
        return record == null;
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {;
        row = new Row(columnName.size());
        for(int i = 0; i < columnName.size(); ++i) {
            String colName = columnName.get(i);
            String val = columnValue.get(i);
            String type = columnType.get(i);
            if(StringUtils.isNotEmpty(colName)) {
                row.setField(i, record.get(colName));
            } else if(val != null && type != null) {
                Object col = StringUtil.string2col(val,type);
                row.setField(i, col);
            } else {
                throw new RuntimeException("Illegal column format");
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
