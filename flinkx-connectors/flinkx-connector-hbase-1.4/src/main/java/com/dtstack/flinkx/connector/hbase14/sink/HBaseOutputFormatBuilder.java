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
import com.dtstack.flinkx.connector.hbase.HBaseMutationConverter;
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

    private final HBaseOutputFormat format;

    public HBaseOutputFormatBuilder() {
        super.format = format = new HBaseOutputFormat();
    }

    public void setTableName(String tableName) {
        format.setTableName(tableName);
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        format.setHbaseConfig(hbaseConfig);
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.setColumnTypes(columnTypes);
    }

    public void setColumnNames(List<String> columnNames) {
        format.setColumnNames(columnNames);
    }

    public void setRowkeyExpress(String rowkeyExpress) {
        format.setRowkeyExpress(rowkeyExpress);
    }

    public void setVersionColumnIndex(Integer versionColumnIndex) {
        format.setVersionColumnIndex(versionColumnIndex);
    }

    public void setVersionColumnValues(String versionColumnValue) {
        format.setVersionColumnValue(versionColumnValue);
    }

    public void setHBaseMutationConverter(HBaseMutationConverter hbaseMutationConverter) {
        format.setMutationConverter(hbaseMutationConverter);
    }

    public void setEncoding(String encoding) {
        if (StringUtils.isEmpty(encoding)) {
            format.setEncoding(HBaseConfigConstants.DEFAULT_ENCODING);
        } else {
            format.setEncoding(encoding);
        }
    }

    public void setWriteBufferSize(Long writeBufferSize) {
        if (writeBufferSize == null || writeBufferSize == 0L) {
            format.setWriteBufferSize(HBaseConfigConstants.DEFAULT_WRITE_BUFFER_SIZE);
        } else {
            format.setWriteBufferSize(writeBufferSize);
        }
    }

    public void setNullMode(String nullMode) {
        if (StringUtils.isEmpty(nullMode)) {
            format.setNullMode(HBaseConfigConstants.DEFAULT_NULL_MODE);
        } else {
            format.setNullMode(nullMode);
        }
    }

    public void setWalFlag(Boolean walFlag) {
        if (walFlag == null) {
            format.setWalFlag(false);
        } else {
            format.setWalFlag(walFlag);
        }
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkArgument(StringUtils.isNotEmpty(format.getTableName()));
        Preconditions.checkNotNull(format.getHbaseConfig());
        Preconditions.checkNotNull(format.getColumnNames());
        Preconditions.checkNotNull(format.getColumnTypes());
    }

    public void setColumnMetaInfos(List<FieldConf> columnMetaInfos) {
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
