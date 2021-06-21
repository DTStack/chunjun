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

package com.dtstack.flinkx.connector.kudu.sink;

import com.dtstack.flinkx.connector.kudu.conf.KuduSinkConf;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class KuduOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private final KuduOutputFormat format;

    public KuduOutputFormatBuilder() {
        super.format = format = new KuduOutputFormat();
    }

    public void setSinkConf(KuduSinkConf sinkConf) {
        super.setConfig(sinkConf);
        format.setKuduSinkConf(sinkConf);
    }

    public void setHadoopConf(Map<String, Object> hadoopConf) {
        format.setHadoopConf(hadoopConf);
    }

    @Override
    protected void checkFormat() {
        KuduSinkConf sinkConf = format.getKuduSinkConf();

        if (StringUtils.isBlank(sinkConf.getMasters())) {
            throw new IllegalArgumentException("Kudu Masters can not be empty.");
        }
    }
}
