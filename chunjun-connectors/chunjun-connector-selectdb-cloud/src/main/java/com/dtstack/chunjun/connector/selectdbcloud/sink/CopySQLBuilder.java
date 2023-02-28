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

package com.dtstack.chunjun.connector.selectdbcloud.sink;

import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConfig;

import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

public class CopySQLBuilder {
    private static final String COPY_SYNC = "copy.async";
    private static final String COPY_DELETE = "copy.use_delete_sign";
    private final SelectdbcloudConfig conf;
    private final String fileName;
    private final Properties properties;

    public CopySQLBuilder(SelectdbcloudConfig conf, String fileName) {
        this.conf = conf;
        this.fileName = fileName;
        this.properties = conf.getLoadProperties();
    }

    public String buildCopySQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ")
                .append(conf.getTableIdentifier())
                .append(" FROM @~('")
                .append(fileName)
                .append("') ")
                .append("PROPERTIES (");

        // copy into must be sync
        properties.put(COPY_SYNC, false);
        if (conf.getEnableDelete()) {
            properties.put(COPY_DELETE, true);
        }
        StringJoiner props = new StringJoiner(",");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            String prop = String.format("'%s'='%s'", key, value);
            props.add(prop);
        }
        sb.append(props).append(" )");
        return sb.toString();
    }
}
