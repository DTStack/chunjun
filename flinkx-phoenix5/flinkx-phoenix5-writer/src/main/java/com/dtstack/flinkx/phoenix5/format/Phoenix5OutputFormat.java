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
package com.dtstack.flinkx.phoenix5.format;

import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.phoenix5.util.PhoenixUtil;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.phoenix.query.QueryServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.URLClassPath;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

/**
 * Company: www.dtstack.com
 *
 * @author wuhui
 */
public class Phoenix5OutputFormat extends JdbcOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Phoenix5OutputFormat.class);

    private static final String PHOENIX5_WRITER_PREFIX = "flinkx-phoenix5-writer";

    @Override
    protected void openInternal(int taskNumber, int numTasks){
        try {
            Field declaredField = ReflectionUtils.getDeclaredField(getClass().getClassLoader(), "ucp");
            declaredField.setAccessible(true);
            URLClassPath urlClassPath = (URLClassPath) declaredField.get(getClass().getClassLoader());
            declaredField.setAccessible(false);

            List<URL> needJar = Lists.newArrayList();
            for(URL url : urlClassPath.getURLs()){
                String urlFileName = FilenameUtils.getName(url.getPath());
                if(urlFileName.startsWith(PHOENIX5_WRITER_PREFIX)){
                    needJar.add(url);
                }
            }

            ClassLoader parentClassLoader = getClass().getClassLoader();
            String[] alwaysParentFirstPatterns = new String[2];
            alwaysParentFirstPatterns[0] = "org.apache.flink";
            alwaysParentFirstPatterns[1] = "com.dtstack.flinkx";
            URLClassLoader childFirstClassLoader = FlinkUserCodeClassLoaders.childFirst(needJar.toArray(new URL[0]), parentClassLoader, alwaysParentFirstPatterns, FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER);

            ClassUtil.forName(driverName, childFirstClassLoader);
            if(StringUtils.isNotEmpty(username)){
                properties.setProperty("user", username);
            }
            if(StringUtils.isNotEmpty(password)){
                properties.setProperty("password", password);
            }
            if(properties.get(QueryServices.MUTATE_BATCH_SIZE_ATTRIB) == null){
                //执行过程中被批处理并自动提交的行数
                properties.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "100000");
            }
            if(properties.get(QueryServices.MAX_MUTATION_SIZE_ATTRIB) == null){
                //客户端批处理的最大行数
                properties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "1000000");
            }
            if(properties.get(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB) == null){
                //客户端批处理的最大数据量（单位：B）1GB
                properties.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "1073741824");
            }
            dbConn = PhoenixUtil.getHelper(childFirstClassLoader).getConn(dbUrl, properties);

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
        } catch (Exception sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

}
