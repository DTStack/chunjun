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
package com.dtstack.flinkx.db2.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Date: 2019/09/20
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Db2InputFormat extends JdbcInputFormat {
    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);
        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if(obj != null) {
                    obj = clobToString(obj);
                }

                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        }catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

    @Override
    public boolean reachedEnd() {
        if (hasNext) {
            return false;
        } else {
            if (incrementConfig.isPolling()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(incrementConfig.getPollingInterval());
                    //间隔轮询检测数据库连接是否断开，超时时间三秒，断开后自动重连
                    boolean valid = false;
                    try{
                        valid = dbConn.isValid(3);
                    }catch (Throwable e){
                        //db2没有数据 就会报错，所以需要catch 不需要打印日志
                    }
                    if(!valid){
                        dbConn = DbUtil.getConnection(dbUrl, username, password);
                        //重新连接后还是不可用则认为数据库异常，任务失败
                        //db2这儿使用isClosed 而不是isvalid 是因为db2没有数据的时候 使用isvalid会抛出异常 因此只能使用connection
                        if(dbConn.isClosed()){
                            String message = String.format("cannot connect to %s, username = %s, please check %s is available.", dbUrl, username, databaseInterface.getDatabaseType());
                            LOG.error(message);
                            throw new RuntimeException(message);
                        }
                    }
                    if(!dbConn.getAutoCommit()){
                        dbConn.setAutoCommit(true);
                    }
                    DbUtil.closeDbResources(resultSet, null, null, false);
                    //此处endLocation理应不会为空
                    queryForPolling(endLocationAccumulator.getLocalValue().toString());
                    return false;
                } catch (InterruptedException e) {
                    LOG.warn("interrupted while waiting for polling, e = {}", ExceptionUtil.getErrorMessage(e));
                } catch (SQLException e) {
                    DbUtil.closeDbResources(resultSet, ps, null, false);
                    String message = String.format("error to execute sql = %s, startLocation = %s, e = %s", querySql, endLocationAccumulator.getLocalValue(), ExceptionUtil.getErrorMessage(e));
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                }
            }
            return true;
        }
    }
}
