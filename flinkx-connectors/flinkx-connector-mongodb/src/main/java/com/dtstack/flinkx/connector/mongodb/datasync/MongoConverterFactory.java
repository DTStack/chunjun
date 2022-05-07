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

package com.dtstack.flinkx.connector.mongodb.datasync;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.mongodb.converter.MongodbColumnConverter;
import com.dtstack.flinkx.connector.mongodb.converter.MongodbRawTypeConverter;
import com.dtstack.flinkx.connector.mongodb.converter.MongodbRowConverter;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/24
 */
public class MongoConverterFactory {

    RowType rowType;
    List<String> fieldNames;
    List<String> fieldTypes;
    MongodbDataSyncConf mongodbDataSyncConf;

    public MongoConverterFactory(MongodbDataSyncConf mongodbDataSyncConf) {
        this.mongodbDataSyncConf = mongodbDataSyncConf;
        fieldNames = new ArrayList<>();
        fieldTypes = new ArrayList<>();
        List<FieldConf> fields = mongodbDataSyncConf.getColumn();
        for (FieldConf field : fields) {
            fieldNames.add(field.getName());
            fieldTypes.add(field.getType());
        }
        rowType = TableUtil.createRowType(fieldNames, fieldTypes, MongodbRawTypeConverter::apply);
    }

    public MongodbRowConverter createRowConverter() {
        return new MongodbRowConverter(rowType, fieldNames.toArray(new String[] {}));
    }

    public MongodbColumnConverter createColumnConverter() {
        return new MongodbColumnConverter(rowType, mongodbDataSyncConf);
    }
}
