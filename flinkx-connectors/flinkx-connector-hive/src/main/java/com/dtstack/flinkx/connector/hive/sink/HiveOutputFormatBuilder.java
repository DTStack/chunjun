/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.connector.hive.sink;

import com.dtstack.flinkx.connector.hive.conf.HiveConf;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormatBuilder;

/**
 * Date: 2021/06/22 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HiveOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    protected HiveOutputFormat format;

    public HiveOutputFormatBuilder() {
        super.format = format = new HiveOutputFormat();
    }

    public void setHiveConf(HiveConf hiveConf) {
        super.setConfig(hiveConf);
        format.setHiveConf(hiveConf);
    }

    @Override
    protected void checkFormat() {
        StringBuilder msg = new StringBuilder(64);
        Integer parallelism = format.getConfig().getParallelism();
        if (parallelism > 1) {
            // 并行度大于1时，移动文件可能存在问题，目前先限制掉，后续优化
            msg.append("Hive sink does not support parallelism setting greater than 1!\n");
        }
        if (msg.length() > 0) {
            throw new IllegalArgumentException(msg.toString());
        }
    }
}
