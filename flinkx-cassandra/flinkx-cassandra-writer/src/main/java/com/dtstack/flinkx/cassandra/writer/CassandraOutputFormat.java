package com.dtstack.flinkx.cassandra.writer;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.dtstack.flinkx.cassandra.CassandraUtil;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.google.common.base.Preconditions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * write plugin for writing static data
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraOutputFormat extends BaseRichOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraOutputFormat.class);

    protected Long batchSize;

    protected List<MetaColumn> columnMeta;

    protected boolean asyncWrite;

    protected String keySpace;

    protected String table;

    protected List<DataType> columnTypes;

    protected String consistancyLevel;

    protected PreparedStatement pstmt;

    protected List<ResultSetFuture> unConfirmedWrite;

    protected List<BoundStatement> bufferedWrite;

    protected Map<String,Object> cassandraConfig;

    protected transient Session session;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        Preconditions.checkNotNull(keySpace, "keySpace must not null!");
        Preconditions.checkNotNull(table, "table must not null");

        LOG.info("taskNumber: {}, numTasks: {}", taskNumber, numTasks);
        session = CassandraUtil.getSession(cassandraConfig, "");
        TableMetadata metadata = session.getCluster().getMetadata().getKeyspace(keySpace).getTable(table);

        columnTypes = new ArrayList<>(columnMeta.size());
        Insert insertStmt = QueryBuilder.insertInto(table);

        for (MetaColumn column : columnMeta) {
            insertStmt.value(column.getName(), QueryBuilder.bindMarker());
            ColumnMetadata col = metadata.getColumn(column.getName());
            if (col == null) {
                throw new RuntimeException("未找到列名" + column.getName());
            }
            columnTypes.add(col.getType());
        }

        if (consistancyLevel != null && !consistancyLevel.isEmpty()) {
            insertStmt.setConsistencyLevel(ConsistencyLevel.valueOf(consistancyLevel));
        } else {
            insertStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        }

        pstmt = session.prepare(insertStmt);

        if(batchSize > 1) {
            if(asyncWrite) {
                unConfirmedWrite = new ArrayList<>();
            } else {
                bufferedWrite = new ArrayList<>();
            }
        }
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        // cassandra支持对重复主键的值覆盖，无需判断writeMode
        BoundStatement boundStatement = pstmt.bind();
        for (int i = 0; i < columnMeta.size(); i++) {
            Object value = row.getField(i);
            try {
                CassandraUtil.bindColumn(boundStatement, i, columnTypes.get(i), value);
            } catch (Exception e) {
                // 包装异常
                throw new WriteRecordException("类型转换失败", e.getCause(), i, row);
            }
        }
        LOG.info("insertSql: {}" + boundStatement);
        session.execute(boundStatement);
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if (batchSize > 1) {
            BoundStatement boundStatement = pstmt.bind();
            for (Row row : rows) {
                for (int i = 0; i < columnMeta.size(); i++) {
                    Object value = row.getField(i);
                    CassandraUtil.bindColumn(boundStatement, i, columnTypes.get(i), value);
                }
                LOG.info("insertSql: {}" + boundStatement);
                if(asyncWrite) {
                    unConfirmedWrite.add(session.executeAsync(boundStatement));
                    if (unConfirmedWrite.size() >= batchSize) {
                        for (ResultSetFuture write : unConfirmedWrite) {
                            write.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
                        }
                        unConfirmedWrite.clear();
                    }
                } else {
                    bufferedWrite.add(boundStatement);
                    if(bufferedWrite.size() >= batchSize) {
                        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                        batchStatement.addAll(bufferedWrite);
                        session.execute(batchStatement);
                        bufferedWrite.clear();
                    }
                }
            }

            // 检查是否还有数据未写出去
            if(unConfirmedWrite != null && unConfirmedWrite.size() > 0) {
                for(ResultSetFuture write : unConfirmedWrite) {
                    write.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
                }
                unConfirmedWrite.clear();
            }
            if(bufferedWrite !=null && bufferedWrite.size() > 0) {
                BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                batchStatement.addAll(bufferedWrite);
                session.execute(batchStatement);
                bufferedWrite.clear();
            }
        }
    }

    @Override
    public void closeInternal() throws IOException {
        CassandraUtil.close(session);
    }
}
