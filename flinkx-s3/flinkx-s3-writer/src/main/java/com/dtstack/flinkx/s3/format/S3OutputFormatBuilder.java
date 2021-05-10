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

package com.dtstack.flinkx.s3.format;

import com.dtstack.flinkx.outputformat.FileOutputFormatBuilder;
import com.dtstack.flinkx.s3.S3Config;

import java.util.List;
/**
 * The builder of FtpOutputFormat
 * <p>
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3OutputFormatBuilder extends FileOutputFormatBuilder {


    public S3OutputFormatBuilder() {
        S3OutputFormat format = new S3OutputFormat();
        super.setFormat(format);
    }

    public void setColumnNames(List<String> columnNames) {
        ((S3OutputFormat)super.format).columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        ((S3OutputFormat)super.format).columnTypes = columnTypes;
    }

    public void setS3Config(S3Config s3Config){
        ((S3OutputFormat)super.format).setS3Config(s3Config);
    }

    @Override
    protected void checkFormat() {
        notSupportBatchWrite("S3Writer");
    }

    public void setObject(List<String> object) {

    }
}
