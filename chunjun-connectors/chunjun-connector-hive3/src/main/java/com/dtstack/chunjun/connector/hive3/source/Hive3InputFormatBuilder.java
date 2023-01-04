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

package com.dtstack.chunjun.connector.hive3.source;

import com.dtstack.chunjun.connector.hive3.config.HdfsConfig;
import com.dtstack.chunjun.connector.hive3.enums.FileType;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

public class Hive3InputFormatBuilder extends BaseRichInputFormatBuilder<BaseHdfsInputFormat> {

    BaseHdfsInputFormat inputFormat;

    public Hive3InputFormatBuilder(String fileType, boolean isHiveTransactionTable) {
        super(null);
        switch (FileType.getByName(fileType)) {
            case ORC:
                if (isHiveTransactionTable) {
                    inputFormat = new HdfsTransactionInputFormat();
                } else {
                    inputFormat = new HdfsOrcInputFormat();
                }
                break;
            case PARQUET:
                inputFormat = new HdfsParquetInputFormat();
                break;
            case TEXT:
                inputFormat = new HdfsTextInputFormat();
                break;
            default:
                throw new UnsupportedTypeException(fileType);
        }
        super.format = this.inputFormat;
    }

    @Override
    protected void checkFormat() {}

    public void setHdfsConf(HdfsConfig hdfsConfig) {
        super.setConfig(hdfsConfig);
        inputFormat.sethdfsConf(hdfsConfig);
    }
}
