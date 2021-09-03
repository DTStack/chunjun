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
package com.dtstack.flinkx.connector.hdfs.source;

import com.dtstack.flinkx.connector.hdfs.conf.HdfsConf;
import com.dtstack.flinkx.connector.hdfs.enums.FileType;
import com.dtstack.flinkx.source.format.BaseRichInputFormatBuilder;

/**
 * Date: 2021/06/08 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsInputFormatBuilder extends BaseRichInputFormatBuilder {

    private final BaseHdfsInputFormat format;

    public HdfsInputFormatBuilder(String type) {
        switch (FileType.getByName(type)) {
            case ORC:
                format = new HdfsOrcInputFormat();
                break;
            case PARQUET:
                format = new HdfsParquetInputFormat();
                break;
            default:
                format = new HdfsTextInputFormat();
        }
        super.format = format;
    }

    public void setHdfsConf(HdfsConf hdfsConf) {
        super.setConfig(hdfsConf);
        format.setHdfsConf(hdfsConf);
    }

    @Override
    protected void checkFormat() {}
}
