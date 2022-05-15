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
package com.dtstack.chunjun.connector.hdfs.sink;

import com.dtstack.chunjun.connector.hdfs.conf.HdfsConf;
import com.dtstack.chunjun.connector.hdfs.enums.FileType;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.sink.format.FileOutputFormatBuilder;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.commons.lang3.StringUtils;

/**
 * Date: 2021/06/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsOutputFormatBuilder extends FileOutputFormatBuilder {
    private final BaseHdfsOutputFormat format;

    public HdfsOutputFormatBuilder(String fileType) {
        switch (FileType.getByName(fileType)) {
            case ORC:
                format = new HdfsOrcOutputFormat();
                break;
            case PARQUET:
                format = new HdfsParquetOutputFormat();
                break;
            default:
                format = new HdfsTextOutputFormat();
        }
        super.setFormat(format);
    }

    public void setHdfsConf(HdfsConf hdfsConf) {
        super.setBaseFileConf(hdfsConf);
        format.setHdfsConf(hdfsConf);
    }

    @Override
    protected void checkFormat() {
        StringBuilder errorMessage = new StringBuilder(256);
        HdfsConf hdfsConf = format.getHdfsConf();
        if (StringUtils.isBlank(hdfsConf.getPath())) {
            errorMessage.append("No path supplied. \n");
        }

        if (StringUtils.isBlank(hdfsConf.getDefaultFS())) {
            errorMessage.append("No defaultFS supplied. \n");
        } else if (!hdfsConf.getDefaultFS().startsWith(ConstantValue.PROTOCOL_HDFS)) {
            errorMessage.append("defaultFS should start with hdfs:// \n");
        }
        if (StringUtils.isNotBlank(errorMessage)) {
            throw new ChunJunRuntimeException(errorMessage.toString());
        }
    }
}
