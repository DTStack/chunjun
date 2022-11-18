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
package com.dtstack.chunjun.connector.hive.sink;

import com.dtstack.chunjun.connector.hive.config.HiveConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.commons.lang3.StringUtils;

public class HiveOutputFormatBuilder extends BaseRichOutputFormatBuilder<HiveOutputFormat> {

    public HiveOutputFormatBuilder() {
        super(new HiveOutputFormat());
    }

    public void setHiveConf(HiveConfig hiveConf) {
        super.setConfig(hiveConf);
        format.setHiveConfig(hiveConf);
    }

    @Override
    protected void checkFormat() {
        StringBuilder errorMessage = new StringBuilder(256);
        HiveConfig hiveConf = format.getHiveConfig();
        if (StringUtils.isBlank(hiveConf.getJdbcUrl())) {
            errorMessage.append("No url supplied. \n");
        }
        if (StringUtils.isBlank(hiveConf.getDefaultFS())) {
            errorMessage.append("No defaultFS supplied. \n");
        } else if (!hiveConf.getDefaultFS().startsWith(ConstantValue.PROTOCOL_HDFS)) {
            errorMessage.append("defaultFS should start with hdfs:// \n");
        }
        if (StringUtils.isBlank(hiveConf.getFileType())) {
            errorMessage.append("No fileType supplied. \n");
        }
        if (StringUtils.isBlank(hiveConf.getTableName())) {
            errorMessage.append("No tableName supplied. \n");
        }
        if (StringUtils.isNotBlank(errorMessage)) {
            throw new ChunJunRuntimeException(errorMessage.toString());
        }
    }
}
