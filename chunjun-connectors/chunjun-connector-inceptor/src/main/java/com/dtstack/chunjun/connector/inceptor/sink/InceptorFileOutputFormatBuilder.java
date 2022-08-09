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

package com.dtstack.chunjun.connector.inceptor.sink;

import com.dtstack.chunjun.connector.inceptor.conf.InceptorFileConf;
import com.dtstack.chunjun.connector.inceptor.enums.ECompressType;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

public class InceptorFileOutputFormatBuilder
        extends BaseRichOutputFormatBuilder<BaseInceptorFileOutputFormat> {

    public InceptorFileOutputFormatBuilder(BaseInceptorFileOutputFormat format) {
        super(format);
    }

    public static InceptorFileOutputFormatBuilder newBuilder(String type) {
        BaseInceptorFileOutputFormat format;
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "ORC":
                format = new InceptorFileOrcOutputFormat();
                break;
            case "PARQUET":
                format = new InceptorFileParquetOutputFormat();
                break;
            default:
                format = new InceptorFileTextOutputFormat();
        }
        return new InceptorFileOutputFormatBuilder(format);
    }

    public void setInceptorConf(InceptorFileConf inceptorFileConf) {
        format.setBaseFileConf(inceptorFileConf);
        format.setInceptorFileConf(inceptorFileConf);
    }

    public void setCompressType(ECompressType compressType) {
        format.setCompressType(compressType);
    }

    @Override
    protected void checkFormat() {
        StringBuilder errorMessage = new StringBuilder(256);
        InceptorFileConf hdfsConf = format.getInceptorFileConf();
        if (StringUtils.isBlank(hdfsConf.getPath())) {
            errorMessage.append("No path supplied. \n");
        }

        if (StringUtils.isBlank(hdfsConf.getDefaultFs())) {
            errorMessage.append("No defaultFS supplied. \n");
        } else if (!hdfsConf.getDefaultFs().startsWith(ConstantValue.PROTOCOL_HDFS)) {
            errorMessage.append("defaultFS should start with hdfs:// \n");
        }
        if (StringUtils.isNotBlank(errorMessage)) {
            throw new ChunJunRuntimeException(errorMessage.toString());
        }
    }
}
