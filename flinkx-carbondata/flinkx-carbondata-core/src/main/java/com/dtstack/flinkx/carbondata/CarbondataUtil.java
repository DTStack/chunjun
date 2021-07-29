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
package com.dtstack.flinkx.carbondata;


import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import java.lang.reflect.Field;
import java.util.Map;

import java.io.IOException;

/**
 * Carbondata Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataUtil {

    private CarbondataUtil() {
        // hehe
    }

    private static SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();

    public static CarbonTable buildCarbonTable(String dbName, String tableName, String tablePath) throws IOException {
        String tableMetadataFile = CarbonTablePath.getSchemaFilePath(tablePath);
        org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.readSchemaFile(tableMetadataFile);
        TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, tablePath);
        return CarbonTable.buildFromTableInfo(wrapperTableInfo);
    }

    public static void initFileFactory(Map<String,String> hadoopConfig, String defaultFs) {
        Configuration conf = new Configuration();
        conf.clear();
        if(hadoopConfig != null) {
            for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }

        if(StringUtils.isNotBlank(defaultFs)) {
            conf.set("fs.default.name", defaultFs);
        }
        conf.set("fs.hdfs.impl.disable.cache", "true");

        try {
            Field confField = FileFactory.class.getDeclaredField("configuration");
            confField.setAccessible(true);
            confField.set(null, conf);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }


}
