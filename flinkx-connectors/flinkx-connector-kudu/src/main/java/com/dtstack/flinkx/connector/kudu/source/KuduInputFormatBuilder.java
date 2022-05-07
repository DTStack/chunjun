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

package com.dtstack.flinkx.connector.kudu.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.kudu.conf.KuduSourceConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.source.format.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.throwable.NoRestartException;

import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/9 星期三
 */
public class KuduInputFormatBuilder extends BaseRichInputFormatBuilder<KuduInputFormat> {

    public KuduInputFormatBuilder() {
        super(new KuduInputFormat());
    }

    public void setKuduSourceConf(KuduSourceConf conf) {
        super.setConfig(conf);
        format.setSourceConf(conf);
    }

    @Override
    protected void checkFormat() {
        KuduSourceConf sourceConf = format.getSourceConf();
        List<FieldConf> columns = sourceConf.getColumn();

        String masters = sourceConf.getMasters();
        StringBuilder sb = new StringBuilder(256);

        if (columns == null || columns.size() == 0) {
            sb.append("Columns can not be empty.\n");
        }

        if (sourceConf.getBatchSizeBytes() > ConstantValue.STORE_SIZE_G) {
            sb.append("BatchSizeBytes must be less than 1G.\n");
        }

        if (masters == null || masters.isEmpty()) {
            sb.append("Kudu masterAddress can not be empty.\n");
        }

        if (sb.length() > 0) {
            throw new NoRestartException(sb.toString());
        }
    }
}
