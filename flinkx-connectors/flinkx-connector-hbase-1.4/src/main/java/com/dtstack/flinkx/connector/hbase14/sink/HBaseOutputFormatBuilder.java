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

package com.dtstack.flinkx.connector.hbase14.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConfigConstants;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormatBuilder;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The Builder class of HbaseOutputFormatBuilder
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HBaseOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private HBaseOutputFormat format;

    public HBaseOutputFormatBuilder() {
        super.format = format = new HBaseOutputFormat();
    }

    public void setTableName(String tableName) {
        format.tableName = tableName;
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        format.hbaseConfig = hbaseConfig;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setRowkeyExpress(String rowkeyExpress) {
        format.rowkeyExpress = rowkeyExpress;
    }

    public void setVersionColumnIndex(Integer versionColumnIndex) {
        format.versionColumnIndex = versionColumnIndex;
    }

    public void setVersionColumnValues(String versionColumnValue) {
        format.versionColumnValue = versionColumnValue;
    }

    public void setEncoding(String encoding) {
        if (StringUtils.isEmpty(encoding)) {
            format.encoding = HBaseConfigConstants.DEFAULT_ENCODING;
        } else {
            format.encoding = encoding;
        }
    }

    public void setWriteBufferSize(Long writeBufferSize) {
        if (writeBufferSize == null || writeBufferSize.longValue() == 0L) {
            format.writeBufferSize = HBaseConfigConstants.DEFAULT_WRITE_BUFFER_SIZE;
        } else {
            format.writeBufferSize = writeBufferSize;
        }
    }

    public void setNullMode(String nullMode) {
        if (StringUtils.isEmpty(nullMode)) {
            format.nullMode = HBaseConfigConstants.DEFAULT_NULL_MODE;
        } else {
            format.nullMode = nullMode;
        }
    }

    public void setWalFlag(Boolean walFlag) {
        if (walFlag == null) {
            format.walFlag = false;
        } else {
            format.walFlag = walFlag;
        }
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkArgument(StringUtils.isNotEmpty(format.tableName));
        Preconditions.checkNotNull(format.hbaseConfig);
        Preconditions.checkNotNull(format.columnNames);
        Preconditions.checkNotNull(format.columnTypes);

        //        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
        //            throw new UnsupportedOperationException("This plugin not support restore from
        // failed state");
        //        }
        //        notSupportBatchWrite("HbaseWriter");
    }

    public void setColumMetaInfos(List<FieldConf> columnMetaInfos) {
        if (columnMetaInfos != null && !columnMetaInfos.isEmpty()) {
            List<String> names =
                    columnMetaInfos.stream().map(FieldConf::getName).collect(Collectors.toList());
            setColumnNames(names);
            List<String> values =
                    columnMetaInfos.stream().map(FieldConf::getType).collect(Collectors.toList());
            setColumnTypes(values);
        }
    }
}
