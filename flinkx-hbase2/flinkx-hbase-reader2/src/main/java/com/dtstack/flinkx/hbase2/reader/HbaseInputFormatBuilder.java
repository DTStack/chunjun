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
package com.dtstack.flinkx.hbase2.reader;

import com.dtstack.flinkx.hbase2.HbaseConfigConstants;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * The builder of HbaseInputFormat
 *
 * Company: cmss
 * @author wangyulei_yewu@cmss.chinamobile.com
 */
public class HbaseInputFormatBuilder extends BaseRichInputFormatBuilder {

    private HbaseInputFormat format;

    public HbaseInputFormatBuilder() {
        super.format = format = new HbaseInputFormat();
    }

    public void setHbaseConfig(Map<String,Object> hbaseConfig) {
        format.hbaseConfig = hbaseConfig;
    }

    public void setTableName(String tableName) {
        format.tableName = tableName;
    }

    public void setStartRowkey(String startRowkey) {
        format.startRowkey = startRowkey;
    }

    public void setEndRowkey(String endRowkey) {
        format.endRowkey = endRowkey;
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
        format.encoding = StringUtils.isEmpty(encoding) ?  "utf-8" : encoding;
    }

    public void setColumnFormats(List<String> columnFormats) {
        format.columnFormats = columnFormats;
    }

    public void setScanCacheSize(int scanCacheSize) {
        format.scanCacheSize = scanCacheSize;
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkNotNull(format.columnTypes);
        Preconditions.checkNotNull(format.columnFormats);
        Preconditions.checkNotNull(format.columnValues);
        Preconditions.checkNotNull(format.columnNames);

        Preconditions.checkArgument(format.scanCacheSize <= HbaseConfigConstants.MAX_SCAN_CACHE_SIZE && format.scanCacheSize >= HbaseConfigConstants.MIN_SCAN_CACHE_SIZE,
                "scanCacheSize should be between " + HbaseConfigConstants.MIN_SCAN_CACHE_SIZE +  " and " + HbaseConfigConstants.MAX_SCAN_CACHE_SIZE);

        for(int i = 0; i < format.columnTypes.size(); ++i) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(format.columnTypes.get(i)));
            Preconditions.checkArgument(StringUtils.isNotEmpty(format.columnNames.get(i))
                    || StringUtils.isNotEmpty(format.columnTypes.get(i)) );
        }

        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
