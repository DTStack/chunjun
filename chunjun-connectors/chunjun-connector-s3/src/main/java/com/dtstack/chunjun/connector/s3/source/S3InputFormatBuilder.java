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

import com.dtstack.chunjun.config.RestoreConfig;
import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.connector.s3.config.S3Config;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class S3InputFormatBuilder extends BaseRichInputFormatBuilder<S3InputFormat> {
    private SpeedConfig speedConfig;

    public S3InputFormatBuilder(S3InputFormat format) {
        super(format);
    }

    public void setS3Conf(S3Config s3Config) {
        super.setConfig(s3Config);
        format.setS3Conf(s3Config);
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(256);
        S3InputFormat s3InputFormat = format;
        S3Config s3Config = (S3Config) s3InputFormat.getConfig();
        if (StringUtils.isBlank(s3Config.getBucket())) {
            log.info("bucket was not supplied separately.");
            sb.append("bucket was not supplied separately;\n");
        }
        if (StringUtils.isBlank(s3Config.getAccessKey())) {
            log.info("accessKey was not supplied separately.");
            sb.append("accessKey was not supplied separately;\n");
        }
        if (StringUtils.isBlank(s3Config.getSecretKey())) {
            log.info("secretKey was not supplied separately.");
            sb.append("secretKey was not supplied separately;\n");
        }
        if (CollectionUtils.isEmpty(s3Config.getObjects())) {
            log.info("objects was not supplied separately.");
            sb.append("objects was not supplied separately;\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public SpeedConfig getSpeedConf() {
        return speedConfig;
    }

    public void setSpeedConf(SpeedConfig speedConfig) {
        this.speedConfig = speedConfig;
    }

    public void setRestoreConf(RestoreConfig restoreConf) {
        ((S3InputFormat) format).setRestoreConf(restoreConf);
    }
}
