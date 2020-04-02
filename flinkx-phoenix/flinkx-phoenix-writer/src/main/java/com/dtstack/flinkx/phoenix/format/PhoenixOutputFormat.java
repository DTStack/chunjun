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
package com.dtstack.flinkx.phoenix.format;

import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.phoenix.util.PhoenixUtil;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Company: www.dtstack.com
 *
 * @author wuhui
 */
public class PhoenixOutputFormat extends JdbcOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixOutputFormat.class);

    @Override
    protected void openInternal(int taskNumber, int numTasks){
        try {
            ClassUtil.forName(driverName, getClass().getClassLoader());
            dbConn = PhoenixUtil.getConnectionInternal(dbUrl, username, password);

            if (restoreConfig.isRestore()){
                dbConn.setAutoCommit(false);
            }

            if(CollectionUtils.isEmpty(fullColumn)) {
                fullColumn = probeFullColumns(table, dbConn);
            }

            if(fullColumnType == null) {
                fullColumnType = analyzeTable();
            }

            for(String col : column) {
                for (int i = 0; i < fullColumn.size(); i++) {
                    if (col.equalsIgnoreCase(fullColumn.get(i))){
                        columnType.add(fullColumnType.get(i));
                        break;
                    }
                }
            }
            mode = EWriteMode.UPDATE.name();
            preparedStatement = prepareTemplates();
            readyCheckpoint = false;

            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

}
