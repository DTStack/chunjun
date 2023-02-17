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

package com.dtstack.chunjun.connector.hbase.util;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;

import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ScanBuilder implements Serializable {

    private static final long serialVersionUID = 6705547370155797433L;

    private final boolean isSync;
    private final HBaseTableSchema hBaseTableSchema;
    private final List<FieldConfig> fieldConfList;

    private ScanBuilder(HBaseTableSchema hBaseTableSchema) {
        this.isSync = false;
        this.fieldConfList = null;
        this.hBaseTableSchema = hBaseTableSchema;
    }

    private ScanBuilder(List<FieldConfig> fieldConfList) {
        this.isSync = true;
        this.fieldConfList = fieldConfList;
        this.hBaseTableSchema = null;
    }

    public static ScanBuilder forSql(HBaseTableSchema hBaseTableSchema) {
        return new ScanBuilder(hBaseTableSchema);
    }

    public static ScanBuilder forSync(List<FieldConfig> fieldConfList) {
        return new ScanBuilder(fieldConfList);
    }

    public Scan buildScan() {
        Scan scan = new Scan();
        if (isSync) {
            assert fieldConfList != null;
            for (FieldConfig fieldConfig : fieldConfList) {
                String fieldName = fieldConfig.getName();
                if (!"rowkey".equalsIgnoreCase(fieldName)) {
                    if (fieldName.contains(".")) {
                        String[] fields = fieldName.split("\\.");
                        scan.addColumn(Bytes.toBytes(fields[0]), Bytes.toBytes(fields[1]));
                    }
                }
            }
            return scan;
        } else {
            assert this.hBaseTableSchema != null;
            String[] familyNames = this.hBaseTableSchema.getFamilyNames();
            for (String familyName : familyNames) {
                Map<String, DataType> familyInfo = hBaseTableSchema.getFamilyInfo(familyName);
                for (Map.Entry<String, DataType> columnInfoEntry : familyInfo.entrySet()) {
                    scan.addColumn(
                            Bytes.toBytes(familyName), Bytes.toBytes(columnInfoEntry.getKey()));
                }
            }
        }
        return scan;
    }
}
