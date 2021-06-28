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

package com.dtstack.flinkx.connector.jdbc.source;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import org.apache.commons.lang.StringUtils;

/**
 * The builder of JdbcInputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcInputFormatBuilder extends BaseRichInputFormatBuilder {

    protected JdbcInputFormat format;

    public JdbcInputFormatBuilder(JdbcInputFormat format) {
        super.format = this.format = format;
    }

    public void setJdbcConf(JdbcConf jdbcConf) {
        super.setConfig(jdbcConf);
        format.setJdbcConf(jdbcConf);
    }

    public void setNumPartitions(int numPartitions) {
        format.setNumPartitions(numPartitions);
    }

    public void setJdbcDialect(JdbcDialect jdbcDialect) {
        format.setJdbcDialect(jdbcDialect);
    }

    @Override
    protected void checkFormat() {
        JdbcConf conf = format.getJdbcConf();
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isBlank(conf.getUsername())) {
            sb.append("No username supplied;\n");
        }
        if (StringUtils.isBlank(conf.getPassword())) {
            sb.append("No password supplied;\n");
        }
        if (StringUtils.isBlank(conf.getJdbcUrl())) {
            sb.append("No jdbc url supplied;\n");
        }
        if (StringUtils.isEmpty(conf.getSplitPk()) && format.getNumPartitions() > 1){
            sb.append("Must specify the split column when the channel is greater than 1;\n");
        }
        if (conf.isPolling() && format.getNumPartitions() > 1){
            sb.append("Interval polling task parallelism cannot be greater than 1;\n");
        }
        if (conf.getFetchSize() > ConstantValue.MAX_BATCH_SIZE) {
            sb.append("The number of fetchSize must be less than [200000];\n");
        }
        if (conf.isIncrement() && conf.isSplitByKey()) {
            sb.append("Must specify the channel equals 1 when the task is increment;\n");
        }
        if(sb.length() > 0){
            throw new IllegalArgumentException(sb.toString());
        }
    }

}
