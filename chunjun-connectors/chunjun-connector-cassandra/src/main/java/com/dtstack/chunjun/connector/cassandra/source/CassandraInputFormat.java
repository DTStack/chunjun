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

package com.dtstack.chunjun.connector.cassandra.source;

import com.dtstack.chunjun.connector.cassandra.config.CassandraSourceConfig;
import com.dtstack.chunjun.connector.cassandra.util.CassandraService;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;

import static com.dtstack.chunjun.connector.cassandra.util.CassandraService.quoteColumn;

@Slf4j
public class CassandraInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = -5354898494437278362L;

    private CassandraSourceConfig sourceConfig;

    private transient Session session;

    protected transient Iterator<Row> cursor;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        ArrayList<CassandraInputSplit> splits = new ArrayList<>();

        try {
            Preconditions.checkNotNull(sourceConfig.getTableName(), "table must not null");
            return CassandraService.splitJob(sourceConfig, minNumSplits, splits);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            CassandraService.close(session);
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        CassandraInputSplit split = (CassandraInputSplit) inputSplit;

        String tableName = sourceConfig.getTableName();
        String keyspaces = sourceConfig.getKeyspaces();

        sourceConfig
                .getColumn()
                .forEach(fieldConfig -> columnNameList.add(quoteColumn(fieldConfig.getName())));

        Preconditions.checkNotNull(tableName, "table must not null");
        session = CassandraService.session(sourceConfig);

        String consistency = sourceConfig.getConsistency();
        ConsistencyLevel consistencyLevel = CassandraService.consistencyLevel(consistency);

        Select select = QueryBuilder.select(columnNameList.toArray()).from(keyspaces, tableName);
        select.setConsistencyLevel(consistencyLevel);
        // TODO where ? group by ? order by ?

        log.info("split: {}, {}", split.getMinToken(), split.getMaxToken());
        ResultSet resultSet = session.execute(select);
        cursor = resultSet.all().iterator();
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            Row cqlRow = cursor.next();
            rowData = rowConverter.toInternal(cqlRow);

        } catch (Exception e) {
            throw new ReadRecordException("Cassandra next record error!", e, -1, rowData);
        }

        return rowData;
    }

    @Override
    protected void closeInternal() {
        CassandraService.close(session);
    }

    @Override
    public boolean reachedEnd() {
        return !cursor.hasNext();
    }

    public CassandraSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(CassandraSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }
}
