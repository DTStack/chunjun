/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.hbase.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfigConstants;
import com.dtstack.chunjun.connector.hbase.util.ScanBuilder;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The builder of HbaseInputFormat */
public class HBaseInputFormatBuilder extends BaseRichInputFormatBuilder<HBaseInputFormat> {

    public static HBaseInputFormatBuilder newBuild(String tableName, ScanBuilder scanBuilder) {
        HBaseInputFormat format = new HBaseInputFormat(tableName, scanBuilder);
        return new HBaseInputFormatBuilder(format);
    }

    public HBaseInputFormatBuilder(HBaseInputFormat format) {
        super(format);
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        format.hbaseConfig = hbaseConfig;
    }

    public void setStartRowKey(String startRowKey) {
        format.startRowkey = startRowKey;
    }

    public void setEndRowKey(String endRowKey) {
        format.endRowkey = endRowKey;
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnValues(List<String> columnValues) {
        format.columnValues = columnValues;
    }

    public void setColumnTypes(List<TypeConfig> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setIsBinaryRowkey(boolean isBinaryRowkey) {
        format.isBinaryRowkey = isBinaryRowkey;
    }

    public void setColumnFormats(List<String> columnFormats) {
        format.columnFormats = columnFormats;
    }

    public void setScanCacheSize(int scanCacheSize) {
        format.scanCacheSize = scanCacheSize;
    }

    public void setScanBatchSize(int scanBatchSize) {
        format.scanBatchSize = scanBatchSize;
    }

    public void setMode(String mode) {
        format.mode = mode;
    }

    public void setMaxVersion(int maxVersion) {
        format.maxVersion = maxVersion;
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkArgument(
                format.scanCacheSize <= HBaseConfigConstants.MAX_SCAN_CACHE_SIZE
                        && format.scanCacheSize >= HBaseConfigConstants.MIN_SCAN_CACHE_SIZE,
                "scanCacheSize should be between "
                        + HBaseConfigConstants.MIN_SCAN_CACHE_SIZE
                        + " and "
                        + HBaseConfigConstants.MAX_SCAN_CACHE_SIZE);

        if (format.columnFormats != null) {
            for (int i = 0; i < format.columnTypes.size(); ++i) {
                Preconditions.checkArgument(
                        StringUtils.isNotEmpty(format.columnTypes.get(i).getType()));
                Preconditions.checkArgument(
                        StringUtils.isNotEmpty(format.columnNames.get(i))
                                || StringUtils.isNotEmpty(format.columnTypes.get(i).getType()));
            }
        }
    }

    public void setColumnMetaInfos(List<FieldConfig> columMetaInfos) {
        if (columMetaInfos != null && !columMetaInfos.isEmpty()) {
            List<String> nameList =
                    columMetaInfos.stream().map(FieldConfig::getName).collect(Collectors.toList());
            setColumnNames(nameList);
            List<TypeConfig> typeList =
                    columMetaInfos.stream().map(FieldConfig::getType).collect(Collectors.toList());
            setColumnTypes(typeList);
            List<String> valueList =
                    columMetaInfos.stream().map(FieldConfig::getValue).collect(Collectors.toList());
            setColumnValues(valueList);
            List<String> formatList =
                    columMetaInfos.stream()
                            .map(FieldConfig::getFormat)
                            .collect(Collectors.toList());
            setColumnFormats(formatList);
        }
    }
}
