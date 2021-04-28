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

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.s3.S3Config;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * build S3InputFormat{@link S3InputFormat} and check S3Config{@link S3Config}
 * <p>
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3InputFormatBuilder extends BaseRichInputFormatBuilder {


    public S3InputFormatBuilder() {
        super.format = new S3InputFormat();
    }


    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(256);
        //todo 检验 objects 是否为 null
        S3InputFormat s3InputFormat = (S3InputFormat) format;
        S3Config s3Config = s3InputFormat.getS3Config();
        if (StringUtils.isBlank(s3Config.getBucket())) {
            LOG.info("bucket was not supplied separately.");
            sb.append("bucket was not supplied separately;\n");
        }
        if (StringUtils.isBlank(s3Config.getAccessKey())) {
            LOG.info("accessKey was not supplied separately.");
            sb.append("accessKey was not supplied separately;\n");
        }
        if (StringUtils.isBlank(s3Config.getSecretKey())) {
            LOG.info("accessKey was not supplied separately.");
            sb.append("accessKey was not supplied separately;\n");
        }
        if (CollectionUtils.isEmpty(s3Config.getObject())) {
            LOG.info("object was not supplied separately.");
            sb.append("object was not supplied separately;\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public void setS3Config(S3Config s3Config) {
        ((S3InputFormat) super.format).setS3Config(s3Config);
    }

    public void setMetaColumn(List<MetaColumn> metaColumns) {
        ((S3InputFormat) super.format).setMetaColumn(metaColumns);
    }

}
