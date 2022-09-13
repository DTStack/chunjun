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

package com.dtstack.chunjun.connector.s3.source;

import com.dtstack.chunjun.conf.RestoreConf;
import com.dtstack.chunjun.conf.SpeedConf;
import com.dtstack.chunjun.connector.s3.conf.S3Conf;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

/**
 * build S3InputFormat{@link S3InputFormat} and check S3Config{@link S3Conf}
 *
 * @author jier
 */
public class S3InputFormatBuilder extends BaseRichInputFormatBuilder {
    private SpeedConf speedConf;

    public S3InputFormatBuilder(S3InputFormat format) {
        super(format);
    }

    public void setS3Conf(S3Conf s3Conf) {
        super.setConfig(s3Conf);
        ((S3InputFormat) format).setS3Conf(s3Conf);
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(256);
        S3InputFormat s3InputFormat = (S3InputFormat) format;
        S3Conf s3Config = (S3Conf) s3InputFormat.getConfig();
        if (StringUtils.isBlank(s3Config.getBucket())) {
            LOG.info("bucket was not supplied separately.");
            sb.append("bucket was not supplied separately;\n");
        }
        if (StringUtils.isBlank(s3Config.getAccessKey())) {
            LOG.info("accessKey was not supplied separately.");
            sb.append("accessKey was not supplied separately;\n");
        }
        if (StringUtils.isBlank(s3Config.getSecretKey())) {
            LOG.info("secretKey was not supplied separately.");
            sb.append("secretKey was not supplied separately;\n");
        }
        if (CollectionUtils.isEmpty(s3Config.getObjects())) {
            LOG.info("objects was not supplied separately.");
            sb.append("objects was not supplied separately;\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public SpeedConf getSpeedConf() {
        return speedConf;
    }

    public void setSpeedConf(SpeedConf speedConf) {
        this.speedConf = speedConf;
    }

    public void setRestoreConf(RestoreConf restoreConf) {
        ((S3InputFormat) format).setRestoreConf(restoreConf);
    }
}
