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

package com.dtstack.flinkx.hdfs.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;

import java.util.List;
import java.util.Map;

/**
 * The Builder of HdfsInputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsInputFormatBuilder extends BaseRichInputFormatBuilder {
    private BaseHdfsInputFormat format;

    public HdfsInputFormatBuilder(String type) {
        switch(type.toUpperCase()) {
            case "TEXT":
                format = new HdfsTextInputFormat();
                break;
            case "ORC":
                format = new HdfsOrcInputFormat();
                break;
            case "PARQUET":
                format = new HdfsParquetInputFormat();
                break;
            default:
                format = new HdfsTextInputFormat();
        }
        super.format = format;
    }

    public void setHadoopConfig(Map<String,Object> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

    public void setFilterRegex(String filterRegex){
        format.filterRegex = filterRegex;
    }

    public void setMetaColumn(List<MetaColumn> metaColumn) {
        format.metaColumns = metaColumn;
    }

    public void setInputPaths(String inputPaths) {
        format.inputPath = inputPaths;
    }

    public void setDelimiter(String delimiter) {
        if(delimiter == null) {
            delimiter = "\\001";
        }
        format.delimiter = delimiter;
    }

    public void setDefaultFs(String defaultFs) {
        format.defaultFs = defaultFs;
    }

    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
