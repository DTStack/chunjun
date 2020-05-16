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

package com.dtstack.flinkx.cassandra.reader;

import com.datastax.driver.core.*;
import com.dtstack.flinkx.cassandra.CassandraUtil;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.google.common.base.Preconditions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Read plugin for reading static data
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraInputFormat extends BaseRichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraInputFormat.class);

    protected String table;

    protected String whereString;

    protected String consistancyLevel;

    protected boolean allowFiltering;

    protected String keySpace;

    protected List<MetaColumn> columnMeta;

    protected Map<String,Object> cassandraConfig;

    protected transient Session session;

    protected transient Iterator<com.datastax.driver.core.Row> cursor;

    @Override
    protected void openInternal(InputSplit inputSplit) {
        CassandraInputSplit split = (CassandraInputSplit) inputSplit;

        Preconditions.checkNotNull(table, "table must not null");
        session = CassandraUtil.getSession(cassandraConfig, "");

        ConsistencyLevel cl;
        if (consistancyLevel != null && !consistancyLevel.isEmpty()) {
            cl = ConsistencyLevel.valueOf(consistancyLevel);
        } else {
            cl = ConsistencyLevel.LOCAL_QUORUM;
        }

        String queryString = getQueryString(split);
        LOG.info("查询SQL: {}", queryString);
        LOG.info("split: {}, {}", split.getMinToken(), split.getMaxToken());
        ResultSet resultSet = session.execute(new SimpleStatement(queryString)
                .setConsistencyLevel(cl));
        cursor = resultSet.all().iterator();
    }

    @Override
    public Row nextRecordInternal(Row row) {
        com.datastax.driver.core.Row cqlRow = cursor.next();
        ColumnDefinitions columnDefinitions = cqlRow.getColumnDefinitions();
        row = new Row(cqlRow.getColumnDefinitions().size());
        List<ColumnDefinitions.Definition> definitions = columnDefinitions.asList();

        for (int i = 0; i < definitions.size(); i++) {
            Object value = CassandraUtil.getData(cqlRow, definitions.get(i).getType(), definitions.get(i).getName());
            row.setField(i, value);
        }
        LOG.info(row.toString());

        return row;
    }

    @Override
    protected void closeInternal() {
        CassandraUtil.close(session);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        ArrayList<CassandraInputSplit> splits = new ArrayList<>();

        try {
            Preconditions.checkNotNull(table, "table must not null");
            return splitJob(minNumSplits, splits);
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            CassandraUtil.close(session);
        }
    }

    /**
     * 分割任务
     * @param minNumSplits 分片数
     * @param splits 分片列表
     * @return 返回InputSplit[]
     */
    private InputSplit[] splitJob(int minNumSplits, ArrayList<CassandraInputSplit> splits) {
        if(minNumSplits <= 1) {
            splits.add(new CassandraInputSplit());
            return splits.toArray(new CassandraInputSplit[splits.size()]);
        }

        if(whereString != null && whereString.toLowerCase().contains(CassandraConstants.TOKEN)) {
            splits.add(new CassandraInputSplit());
            return splits.toArray(new CassandraInputSplit[splits.size()]);
        }
        Session session = CassandraUtil.getSession(cassandraConfig, "");
        String partitioner = session.getCluster().getMetadata().getPartitioner();
        if( partitioner.endsWith(CassandraConstants.RANDOM_PARTITIONER)) {
            BigDecimal minToken = BigDecimal.valueOf(-1);
            BigDecimal maxToken = new BigDecimal(new BigInteger("2").pow(127));
            BigDecimal step = maxToken.subtract(minToken)
                    .divide(BigDecimal.valueOf(minNumSplits),2, BigDecimal.ROUND_HALF_EVEN);
            for ( int i = 0; i < minNumSplits; i++ ) {
                BigInteger l = minToken.add(step.multiply(BigDecimal.valueOf(i))).toBigInteger();
                BigInteger r = minToken.add(step.multiply(BigDecimal.valueOf(i+1))).toBigInteger();
                if( i == minNumSplits - 1 ) {
                    r = maxToken.toBigInteger();
                }
                splits.add(new CassandraInputSplit(l.toString(), r.toString()));
            }
        }
        else if(partitioner.endsWith(CassandraConstants.MURMUR3_PARTITIONER)) {
            BigDecimal minToken = BigDecimal.valueOf(Long.MIN_VALUE);
            BigDecimal maxToken = BigDecimal.valueOf(Long.MAX_VALUE);
            BigDecimal step = maxToken.subtract(minToken)
                    .divide(BigDecimal.valueOf(minNumSplits),2, BigDecimal.ROUND_HALF_EVEN);
            for ( int i = 0; i < minNumSplits; i++ ) {
                long l = minToken.add(step.multiply(BigDecimal.valueOf(i))).longValue();
                long r = minToken.add(step.multiply(BigDecimal.valueOf(i+1))).longValue();
                if( i == minNumSplits - 1 ) {
                    r = maxToken.longValue();
                }
                splits.add(new CassandraInputSplit(String.valueOf(l), String.valueOf(r)));
            }
        }
        else {
            splits.add(new CassandraInputSplit());
        }
        return splits.toArray(new CassandraInputSplit[splits.size()]);
    }

    /**
     * 拼接查询语句
     * @param inputSplit 分片
     * @return 返回查询语句
     */
    private String getQueryString(CassandraInputSplit inputSplit) {
        StringBuilder columns = new StringBuilder();
        if (columnMeta == null) {
            columns.append(ConstantValue.STAR_SYMBOL);
        } else {
            for(MetaColumn column : columnMeta) {
                if(columns.length() > 0 ) {
                    columns.append(",");
                }
                columns.append(column.getName());
            }
        }

        StringBuilder where = new StringBuilder();

        if( whereString != null && !whereString.isEmpty() ) {
            where.append(whereString);
        }
        String minToken = inputSplit.getMinToken();
        String maxToken = inputSplit.getMaxToken();
        if(minToken !=null || maxToken !=null) {
            LOG.info("range:" + minToken + "~" + maxToken);
            List<ColumnMetadata> pks = session.getCluster().getMetadata().getKeyspace(keySpace).getTable(table)
                    .getPartitionKey();
            StringBuilder sb = new StringBuilder();
            for(ColumnMetadata pk : pks) {
                if( sb.length() > 0 ) {
                    sb.append(",");
                }
                sb.append(pk.getName());
            }
            String s = sb.toString();
            if (minToken != null && !minToken.isEmpty()) {
                if( where.length() > 0 ){
                    where.append(" AND ");
                }
                where.append("token(").append(s).append(")").append(" > ").append(minToken);
            }
            if (maxToken != null && !maxToken.isEmpty()) {
                if( where.length() > 0 ){
                    where.append(" AND ");
                }
                where.append("token(").append(s).append(")").append(" <= ").append(maxToken);
            }
        }

        StringBuilder select = new StringBuilder();
        select.append("SELECT ").append(columns.toString()).append(" FROM ").append(table);
        if( where.length() > 0 ){
            select.append(" where ").append(where.toString());
        }
        if(allowFiltering) {
            select.append(" ALLOW FILTERING");
        }
        select.append(";");
        return select.toString();
    }

    @Override
    public boolean reachedEnd() {
        return !cursor.hasNext();
    }
}
