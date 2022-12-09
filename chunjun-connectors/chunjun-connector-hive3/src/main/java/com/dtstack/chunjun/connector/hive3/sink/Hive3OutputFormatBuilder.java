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

package com.dtstack.chunjun.connector.hive3.sink;

import com.dtstack.chunjun.connector.hive3.config.HdfsConfig;
import com.dtstack.chunjun.connector.hive3.enums.FileType;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

public class Hive3OutputFormatBuilder extends BaseRichOutputFormatBuilder<BaseHdfsOutputFormat> {

    public Hive3OutputFormatBuilder(BaseHdfsOutputFormat format) {
        super(format);
    }

    public static Hive3OutputFormatBuilder newBuild(String type) {
        BaseHdfsOutputFormat format;
        switch (FileType.getByName(type)) {
            case ORC:
                format = new HdfsOrcOutputFormat();
                break;
            case PARQUET:
                format = new HdfsParquetOutputFormat();
                break;
            default:
                format = new HdfsTextOutputFormat();
        }
        return new Hive3OutputFormatBuilder(format);
    }

    public void setHdfsConf(HdfsConfig hdfsConfig) {
        super.setConfig(hdfsConfig);
        format.setBaseFileConfig(hdfsConfig);
        format.setHdfsConf(hdfsConfig);
    }

    @Override
    protected void checkFormat() {}
}
