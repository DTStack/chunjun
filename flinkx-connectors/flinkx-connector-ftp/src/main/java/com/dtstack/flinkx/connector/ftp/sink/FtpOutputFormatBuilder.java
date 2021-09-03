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

package com.dtstack.flinkx.connector.ftp.sink;

import com.dtstack.flinkx.connector.ftp.conf.FtpConfig;
import com.dtstack.flinkx.sink.format.FileOutputFormatBuilder;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;

import org.apache.commons.lang.StringUtils;

/**
 * The builder of FtpOutputFormat
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class FtpOutputFormatBuilder extends FileOutputFormatBuilder {

    private FtpOutputFormat format;

    public FtpOutputFormatBuilder() {
        format = new FtpOutputFormat();
        super.setFormat(format);
    }

    public void setFtpConfig(FtpConfig ftpConfig) {
        super.setBaseFileConf(ftpConfig);
        format.setFtpConfig(ftpConfig);
    }

    @Override
    protected void checkFormat() {
        FtpConfig ftpConfig = format.getFtpConfig();
        if (StringUtils.isBlank(ftpConfig.getProtocol())) {
            throw new FlinkxRuntimeException("Please Set protocol");
        }
        if (StringUtils.isBlank(ftpConfig.getHost())) {
            throw new FlinkxRuntimeException("Please Set gost");
        }
        if (StringUtils.isBlank(ftpConfig.getPath())) {
            throw new FlinkxRuntimeException("Please Set path");
        }
    }
}
