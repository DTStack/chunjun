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

package com.dtstack.chunjun.connector.cassandra.sink;

import com.dtstack.chunjun.connector.cassandra.conf.CassandraSinkConf;
import com.dtstack.chunjun.connector.cassandra.util.CassandraService;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.dtstack.chunjun.connector.cassandra.util.CassandraService.quoteColumn;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraOutputFormat extends BaseRichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraOutputFormat.class);

    private static final long serialVersionUID = 1L;

    private CassandraSinkConf sinkConf;

    private Session session;

    private PreparedStatement preparedStatement;

    protected List<ResultSetFuture> unConfirmedWrite;

    protected List<BoundStatement> bufferedWrite;

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            // cassandra支持对重复主键的值覆盖，无需判断writeMode
            BoundStatement boundStatement = preparedStatement.bind();
            BoundStatement statement =
                    (BoundStatement) rowConverter.toExternal(rowData, boundStatement);
            session.execute(statement);
        } catch (Exception e) {
            throw new WriteRecordException("", e, -1, rowData);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void writeMultipleRecordsInternal() throws Exception {
        if (batchSize > 1) {
            BoundStatement boundStatement = preparedStatement.bind();
            for (RowData rowData : rows) {

                BoundStatement statement =
                        (BoundStatement) rowConverter.toExternal(rowData, boundStatement);

                if (sinkConf.isAsyncWrite()) {
                    unConfirmedWrite.add(session.executeAsync(statement));
                    if (unConfirmedWrite.size() >= batchSize) {
                        for (ResultSetFuture write : unConfirmedWrite) {
                            write.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
                        }
                        unConfirmedWrite.clear();
                    }
                } else {
                    bufferedWrite.add(statement);
                    if (bufferedWrite.size() >= batchSize) {
                        BatchStatement batchStatement =
                                new BatchStatement(BatchStatement.Type.UNLOGGED);
                        batchStatement.addAll(bufferedWrite);
                        session.execute(batchStatement);
                        bufferedWrite.clear();
                    }
                }
            }

            // 检查是否还有数据未写出去
            if (unConfirmedWrite != null && unConfirmedWrite.size() > 0) {
                for (ResultSetFuture write : unConfirmedWrite) {
                    write.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
                }
                unConfirmedWrite.clear();
            }
            if (bufferedWrite != null && bufferedWrite.size() > 0) {
                BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                batchStatement.addAll(bufferedWrite);
                session.execute(batchStatement);
                bufferedWrite.clear();
            }
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        LOG.info("taskNumber: {}, numTasks: {}", taskNumber, numTasks);

        String keyspaces = sinkConf.getKeyspaces();
        String table = sinkConf.getTableName();

        ConsistencyLevel consistencyLevel =
                CassandraService.consistencyLevel(sinkConf.getConsistency());

        session = CassandraService.session(sinkConf);

        Insert insert = QueryBuilder.insertInto(keyspaces, table);

        TableMetadata metadata =
                session.getCluster().getMetadata().getKeyspace(keyspaces).getTable(table);
        metadata.getColumns()
                .forEach(
                        columnMetadata ->
                                insert.value(
                                        quoteColumn(columnMetadata.getName()),
                                        QueryBuilder.bindMarker()));

        insert.setConsistencyLevel(consistencyLevel);

        preparedStatement = session.prepare(insert);

        if (batchSize > 1) {
            if (sinkConf.isAsyncWrite()) {
                unConfirmedWrite = new ArrayList<>();
            } else {
                bufferedWrite = new ArrayList<>();
            }
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        CassandraService.close(session);
    }

    public CassandraSinkConf getSinkConf() {
        return sinkConf;
    }

    public void setSinkConf(CassandraSinkConf sinkConf) {
        this.sinkConf = sinkConf;
    }
}
