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
package com.dtstack.flinkx.connector.hbase14.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConfigConstants;
import com.dtstack.flinkx.source.format.BaseRichInputFormatBuilder;

import org.apache.flink.util.Preconditions;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The builder of HbaseInputFormat
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HBaseInputFormatBuilder extends BaseRichInputFormatBuilder {

    private final HBaseInputFormat format;

    public HBaseInputFormatBuilder() {
        super.format = format = new HBaseInputFormat();
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        format.hbaseConfig = hbaseConfig;
    }

    public void setTableName(String tableName) {
        format.tableName = tableName;
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

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setIsBinaryRowkey(boolean isBinaryRowkey) {
        format.isBinaryRowkey = isBinaryRowkey;
    }

    public void setEncoding(String encoding) {
        format.encoding = StringUtils.isEmpty(encoding) ? "utf-8" : encoding;
    }

    public void setColumnFormats(List<String> columnFormats) {
        format.columnFormats = columnFormats;
    }

    public void setScanCacheSize(int scanCacheSize) {
        format.scanCacheSize = scanCacheSize;
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
                Preconditions.checkArgument(StringUtils.isNotEmpty(format.columnTypes.get(i)));
                Preconditions.checkArgument(
                        StringUtils.isNotEmpty(format.columnNames.get(i))
                                || StringUtils.isNotEmpty(format.columnTypes.get(i)));
            }
        }
    }

    public void setColumnMetaInfos(List<FieldConf> columMetaInfos) {
        if (columMetaInfos != null && !columMetaInfos.isEmpty()) {
            List<String> nameList =
                    columMetaInfos.stream().map(FieldConf::getName).collect(Collectors.toList());
            setColumnNames(nameList);
            List<String> typeList =
                    columMetaInfos.stream().map(FieldConf::getType).collect(Collectors.toList());
            setColumnTypes(typeList);
            List<String> valueList =
                    columMetaInfos.stream().map(FieldConf::getValue).collect(Collectors.toList());
            setColumnValues(valueList);
            List<String> formatList =
                    columMetaInfos.stream().map(FieldConf::getFormat).collect(Collectors.toList());
            setColumnFormats(formatList);
        }
    }
}
