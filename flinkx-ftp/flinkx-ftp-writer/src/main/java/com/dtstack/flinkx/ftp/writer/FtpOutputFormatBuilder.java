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

package com.dtstack.flinkx.ftp.writer;

import com.dtstack.flinkx.ftp.FtpConfig;
import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.outputformat.FileOutputFormatBuilder;
import org.apache.commons.lang.StringUtils;
import java.util.List;

/**
 * The builder of FtpOutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpOutputFormatBuilder extends FileOutputFormatBuilder {

    private FtpOutputFormat format;

    public FtpOutputFormatBuilder() {
        format = new FtpOutputFormat();
        super.setFormat(format);
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setFtpConfig(FtpConfig ftpConfig){
        format.ftpConfig = ftpConfig;
    }

    @Override
    protected void checkFormat() {

    }

}
