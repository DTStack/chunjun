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

package com.dtstack.flinkx.connector.phoenix5.source;

import com.dtstack.flinkx.connector.phoenix5.conf.Phoenix5Conf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.source.format.BaseRichInputFormatBuilder;

import org.apache.commons.lang.StringUtils;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class HBaseInputFormatBuilder extends BaseRichInputFormatBuilder {

    protected HBaseInputFormat format;

    public HBaseInputFormatBuilder() {
        super.format = this.format = new HBaseInputFormat();
    }

    public void setPhoenix5Conf(Phoenix5Conf phoenix5Conf) {
        super.setConfig(phoenix5Conf);
        format.setPhoenix5Conf(phoenix5Conf);
        format.setJdbcConf(phoenix5Conf);
    }

    @Override
    protected void checkFormat() {
        Phoenix5Conf conf = format.getPhoenix5Conf();
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isBlank(conf.getJdbcUrl())) {
            sb.append("No jdbc url supplied;\n");
        }
        if (conf.getFetchSize() > ConstantValue.MAX_BATCH_SIZE) {
            sb.append("The number of fetchSize must be less than [200000];\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
