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

import com.dtstack.chunjun.connector.hive3.conf.HdfsConf;
import com.dtstack.chunjun.connector.hive3.enums.FileType;
import com.dtstack.chunjun.sink.format.FileOutputFormatBuilder;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

/** @author liuliu 2022/3/23 */
public class Hive3OutputFormatBuilder extends FileOutputFormatBuilder {
    BaseHdfsOutputFormat outputFormat;

    public Hive3OutputFormatBuilder(String fileType, boolean isHiveTransactionTable) {
        switch (FileType.getByName(fileType)) {
            case ORC:
                if (isHiveTransactionTable) {
                    outputFormat = new HdfsTransactionOutputFormat();
                } else {
                    outputFormat = new HdfsOrcOutputFormat();
                }
                break;
            case PARQUET:
                outputFormat = new HdfsParquetOutputFormat();
                break;
            case TEXT:
                outputFormat = new HdfsTextOutputFormat();
                break;
            default:
                throw new UnsupportedTypeException(fileType);
        }
        super.setFormat(outputFormat);
    }

    public void setHdfsConf(HdfsConf hdfsConf) {
        super.setBaseFileConf(hdfsConf);
        outputFormat.setHdfsConf(hdfsConf);
    }

    @Override
    protected void checkFormat() {}
}
