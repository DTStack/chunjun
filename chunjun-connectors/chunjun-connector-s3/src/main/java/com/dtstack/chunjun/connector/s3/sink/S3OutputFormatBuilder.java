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

package com.dtstack.chunjun.connector.s3.sink;

import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.connector.s3.config.S3Config;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/** The builder of FtpOutputFormat */
@Slf4j
public class S3OutputFormatBuilder extends BaseRichOutputFormatBuilder<S3OutputFormat> {

    private SpeedConfig speedConfig;

    public S3OutputFormatBuilder(S3OutputFormat format) {
        super(format);
    }

    @Override
    protected void checkFormat() {

        StringBuilder sb = new StringBuilder(256);
        S3Config s3Config = (S3Config) format.getConfig();
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
        if (StringUtils.isBlank(s3Config.getObject())) {
            log.info("object was not supplied separately.");
            sb.append("object was not supplied separately;\n");
        }
        if (speedConfig.getChannel() > 1) {
            sb.append(
                    String.format(
                            "S3Writer can not support channel bigger than 1, current channel is [%s]",
                            speedConfig.getChannel()));
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public void setS3Conf(S3Config conf) {
        super.setConfig(conf);
        format.setS3Conf(conf);
    }

    public void setSpeedConf(SpeedConfig speedConfig) {
        this.speedConfig = speedConfig;
    }
}
