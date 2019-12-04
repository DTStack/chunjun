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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.dtstack.flinkx.cassandra.CassandraUtil;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.esotericsoftware.minlog.Log;
import com.google.common.base.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

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
public class CassandraInputFormat extends RichInputFormat {

    protected String hostPorts;

    protected String username;

    protected String password;

    protected String table;

    protected Map<String,Object> cassandraConfig;

    protected transient Session session;
    protected transient Iterator<com.datastax.driver.core.Row> cursor;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        CassandraInputSplit split = (CassandraInputSplit) inputSplit;

        Preconditions.checkNotNull(table, "table must not null");
        session = CassandraUtil.getSession(cassandraConfig, "");

        String query = "SELECT * FROM " + table;

        ResultSet resultSet = session.execute(query);
        List<com.datastax.driver.core.Row> rowList =  resultSet.all();
        int start = split.getSkip();
        int end = split.getLimit();
        cursor = rowList.subList(start, end).listIterator();
    }

    @Override
    public Row nextRecordInternal(Row row) {
        com.datastax.driver.core.Row cqlRow = cursor.next();
        ColumnDefinitions columnDefinitions = cqlRow.getColumnDefinitions();
        row = new Row(cqlRow.getColumnDefinitions().size());
        List<ColumnDefinitions.Definition> definitions = columnDefinitions.asList();

        for (int i = 0; i < definitions.size(); i++) {
            Object value = CassandraUtil.getData(cqlRow, definitions.get(i).getType(), definitions.get(i).getName());
            Log.error(value + " ");
            row.setField(i, value);
        }

        return row;
    }

    @Override
    protected void closeInternal() {
        CassandraUtil.close(session);
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        ArrayList<CassandraInputSplit> splits = new ArrayList<>();

        Session session = null;
        try {
            Preconditions.checkNotNull(table, "table must not null");
            session = CassandraUtil.getSession(cassandraConfig, "");

            String query = "SELECT * FROM " + table;
            ResultSet resultSet = session.execute(query);

            long docNum = resultSet.all().size();
            if(docNum <= minNumSplits){
                splits.add(new CassandraInputSplit(0,(int)docNum));
                return splits.toArray(new CassandraInputSplit[splits.size()]);
            }

            long size = Math.floorDiv(docNum,(long)minNumSplits);
            for (int i = 0; i < minNumSplits; i++) {
                splits.add(new CassandraInputSplit((int)(i * size), (int)size));
            }

            if(size * minNumSplits < docNum){
                splits.add(new CassandraInputSplit((int)(size * minNumSplits), (int)(docNum - size * minNumSplits)));
            }
        } catch (Exception e){
            LOG.error("{}", e);
        } finally {
            CassandraUtil.close(session);
        }

        return splits.toArray(new CassandraInputSplit[splits.size()]);
    }

    @Override
    public boolean reachedEnd() {
        return !cursor.hasNext();
    }
}
