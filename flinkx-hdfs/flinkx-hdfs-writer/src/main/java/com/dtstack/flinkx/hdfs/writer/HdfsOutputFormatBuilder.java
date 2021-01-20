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

package com.dtstack.flinkx.hdfs.writer;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.hdfs.HaConfigKeys;
import com.dtstack.flinkx.outputformat.FileOutputFormatBuilder;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * The builder class of HdfsOutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsOutputFormatBuilder extends FileOutputFormatBuilder {

    private BaseHdfsOutputFormat format;

    public HdfsOutputFormatBuilder(String type) {
        switch(type.toUpperCase()) {
            case "TEXT":
                format = new HdfsTextOutputFormat();
                break;
            case "ORC":
                format = new HdfsOrcOutputFormat();
                break;
            case "PARQUET":
                format = new HdfsParquetOutputFormat();
                break;
            default:
                throw new IllegalArgumentException("Unsupported HDFS file type: " + type);
        }

        super.setFormat(format);
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setHadoopConfig(Map<String,Object> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

    public void setIsHa(boolean isHa) {
        format.isHa = isHa;
    }

    public void setFullColumnNames(List<String> fullColumnNames) {
        format.fullColumnNames = fullColumnNames;
    }

    public void setDelimiter(String delimiter) {
        format.delimiter = delimiter;
    }

    public void setRowGroupSize(int rowGroupSize){
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

        if (format.getPath() == null || format.getPath().length() == 0) {
            errorMessage.append("No path supplied. \n");
        }

        if (StringUtils.isBlank(format.defaultFs)) {
            errorMessage.append("No defaultFS supplied. \n");
        }else if (!format.defaultFs.startsWith(ConstantValue.PROTOCOL_HDFS)) {
            errorMessage.append("defaultFS should start with hdfs:// \n");
        }

        if (format.isHa) {
            if (MapUtils.isEmpty(format.hadoopConfig)) {
                errorMessage.append("hadoopConfig not allow null when hadoop environment is ha \n");
            } else {
                String template = " param 【%s】 in hadoopConfig not allow null when hadoop environment is ha \n";
                if (StringUtils.isBlank((String) format.hadoopConfig.get(HaConfigKeys.KEY_NAME_SERVICES))) {
                    errorMessage.append(String.format(template, HaConfigKeys.KEY_NAME_SERVICES));
                }

                if (StringUtils.isBlank((String) format.hadoopConfig.get(HaConfigKeys.KEY_NAME_NODES + format.hadoopConfig.get(HaConfigKeys.KEY_NAME_SERVICES)))) {
                    errorMessage.append(String.format(template, HaConfigKeys.KEY_NAME_NODES + format.hadoopConfig.get(HaConfigKeys.KEY_NAME_SERVICES)));
                }

                if (StringUtils.isBlank((String) format.hadoopConfig.get(HaConfigKeys.KEY__PROXY_PROVIDER + format.hadoopConfig.get(HaConfigKeys.KEY_NAME_SERVICES)))) {
                    errorMessage.append(String.format(template, HaConfigKeys.KEY__PROXY_PROVIDER + format.hadoopConfig.get(HaConfigKeys.KEY_NAME_SERVICES)));
                }


                if (format.hadoopConfig.keySet().stream().noneMatch(i->i.contains(HaConfigKeys.KEY_RPC_ADDRESS))) {
                    errorMessage.append(String.format(template, HaConfigKeys.KEY_RPC_ADDRESS));
                }

                if (errorMessage.length() != 0) {
                    throw new IllegalArgumentException(errorMessage.toString());
                }
            }
        }
    }

}
