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

package com.dtstack.flinkx.inceptor.writer;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.outputformat.FileOutputFormatBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.inceptor.HdfsConfigKeys.KEY_SCHEMA;
import static com.dtstack.flinkx.inceptor.HdfsConfigKeys.KEY_TABLE;

/**
 * The builder class of HdfsOutputFormat
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class InceptorOutputFormatBuilder extends FileOutputFormatBuilder {

    private BaseInceptorOutputFormat format;

    public InceptorOutputFormatBuilder(String type) {
        switch (type.toUpperCase()) {
            case "ORC":
                format = new InceptorOrcOutputFormat();
                break;
            default:
                throw new IllegalArgumentException("Unsupported HDFS file type: " + type);
        }

        super.setFormat(format);
    }

    public void setTransaction(Boolean isTransaction) {
        format.isTransaction = isTransaction;
    }

    public void setPartitions(List<String> partitions) {
        format.partitions = partitions;
    }


    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

    public void setFullColumnNames(List<String> fullColumnNames) {
        format.fullColumnNames = fullColumnNames;
    }

    public void setDelimiter(String delimiter) {
        format.delimiter = delimiter;
    }

    public void setRowGroupSize(int rowGroupSize) {
        format.rowGroupSize = rowGroupSize;
    }

    public void setFullColumnTypes(List<String> fullColumnTypes) {
        format.fullColumnTypes = fullColumnTypes;
    }

    public void setDefaultFs(String defaultFs) {
        format.defaultFs = defaultFs;
    }

    public void setEnableDictionary(boolean enableDictionary) {
        format.enableDictionary = enableDictionary;
    }

    @Override
    protected void checkFormat() {
        StringBuilder errorMessage = new StringBuilder(256);
        String table = (String)format.hadoopConfig.getOrDefault(KEY_TABLE, "");
        if (StringUtils.isEmpty(table)) {
            errorMessage.append("table param is must");
        }
        String schema = (String)format.hadoopConfig.getOrDefault(KEY_SCHEMA, "");
        if (StringUtils.isEmpty(schema)) {
            errorMessage.append("schema param is must");
        }
        if (format.isTransaction) {
            format.setRestoreState(null);
        }
        if ((format.getPath() == null || format.getPath().length() == 0) && (!format.isTransaction)) {
            errorMessage.append("No path supplied. \n");
        }
        if (StringUtils.isBlank(format.defaultFs)) {
            errorMessage.append("No defaultFS supplied. \n");
        } else if (!format.defaultFs.startsWith(ConstantValue.PROTOCOL_HDFS)) {
            errorMessage.append("defaultFS should start with hdfs:// \n");
        }
        if (errorMessage.length() > 0) {
            throw new IllegalArgumentException(errorMessage.toString());
        }
    }



}
