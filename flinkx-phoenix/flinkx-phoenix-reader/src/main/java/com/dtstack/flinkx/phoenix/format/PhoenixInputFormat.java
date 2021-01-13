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

import com.dtstack.flinkx.phoenix.util.PhoenixUtil;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import sun.misc.URLClassPath;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Company: www.dtstack.com
 *
 * @author wuhui
 */
public class PhoenixInputFormat extends JdbcInputFormat {

    private static final String PHOENIX_READER_PREFIX = "flinkx-phoenix-reader";

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info(inputSplit.toString());

            Field declaredField = ReflectionUtils.getDeclaredField(getClass().getClassLoader(), "ucp");
            declaredField.setAccessible(true);
            URLClassPath urlClassPath = (URLClassPath) declaredField.get(getClass().getClassLoader());
            declaredField.setAccessible(false);

            List<URL> needJar = Lists.newArrayList();
            for (URL url : urlClassPath.getURLs()) {
                String urlFileName = FilenameUtils.getName(url.getPath());
                if (urlFileName.startsWith(PHOENIX_READER_PREFIX)) {
                    needJar.add(url);
                }
            }

            ClassLoader parentClassLoader = getClass().getClassLoader();
            String[] alwaysParentFirstPatterns = new String[2];
            alwaysParentFirstPatterns[0] = "org.apache.flink";
            alwaysParentFirstPatterns[1] = "com.dtstack.flinkx";
            URLClassLoader childFirstClassLoader = FlinkUserCodeClassLoaders.childFirst(needJar.toArray(new URL[0]), parentClassLoader, alwaysParentFirstPatterns, FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER);

            ClassUtil.forName(driverName, childFirstClassLoader);

            if (incrementConfig.isIncrement() && incrementConfig.isUseMaxFunc()) {
                getMaxValue(inputSplit);
            }

            initMetric(inputSplit);

            if (!canReadData(inputSplit)) {
                LOG.warn("Not read data when the start location are equal to end location");

                hasNext = false;
                return;
            }

            dbConn = PhoenixUtil.getConnectionInternal(dbUrl, username, password, childFirstClassLoader);

            // 部分驱动需要关闭事务自动提交，fetchSize参数才会起作用
            dbConn.setAutoCommit(false);

            statement = dbConn.createStatement(resultSetType, resultSetConcurrency);

            statement.setFetchSize(fetchSize);

            statement.setQueryTimeout(queryTimeOut);
            String querySql = buildQuerySql(inputSplit);
            resultSet = statement.executeQuery(querySql);
            columnCount = resultSet.getMetaData().getColumnCount();

            boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
            if (splitWithRowCol) {
                columnCount = columnCount - 1;
            }

            hasNext = resultSet.next();

            if (StringUtils.isEmpty(customSql)) {
                columnTypeList = PhoenixUtil.analyzeTable(resultSet, metaColumns);
            } else {
                columnTypeList = new ArrayList<>();
                for (MetaColumn metaColumn : metaColumns) {
                    columnTypeList.add(metaColumn.getName());
                }
            }

        } catch (Exception se) {
            throw new IllegalArgumentException("open() failed. " + se.getMessage(), se);
        }

        LOG.info("JdbcInputFormat[{}]open: end", jobName);
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);

        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if (obj != null) {
                    if (CollectionUtils.isNotEmpty(columnTypeList)) {
                        String columnType = columnTypeList.get(pos);
                        if ("year".equalsIgnoreCase(columnType)) {
                            java.util.Date date = (java.util.Date) obj;
                            obj = DateUtil.dateToYearString(date);
                        } else if ("tinyint".equalsIgnoreCase(columnType)
                                || "bit".equalsIgnoreCase(columnType)) {
                            if (obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        }
                    }
                    obj = clobToString(obj);
                }

                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        } catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }
}
