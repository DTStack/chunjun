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

package com.dtstack.flinkx.config;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The configuration of writer configuration
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class WriterConfig extends AbstractConfig {

    public static String KEY_PARAMETER_CONFIG = "parameter";
    public static String KEY_WRITER_NAME = "name";

    ParameterConfig parameter;

    public WriterConfig(Map<String, Object> map) {
        super(map);
        parameter = new ParameterConfig((Map<String, Object>) getVal(KEY_PARAMETER_CONFIG));
    }

    public String getName() {
        return getStringVal(KEY_WRITER_NAME);
    }

    public void setName(String name) {
        setStringVal(KEY_WRITER_NAME, name);
    }

    public ParameterConfig getParameter() {
        return parameter;
    }

    public void setParameter(ParameterConfig parameter) {
        this.parameter = parameter;
    }

    public static class ParameterConfig extends AbstractConfig {
        public static final String KEY_COLUMN_LIST = "column";
        public static final String KEY_CONNECTION_CONFIG_LIST = "connection";

        List column;
        List<ConnectionConfig> connection;

        public ParameterConfig(Map<String, Object> map) {
            super(map);
            column = (List) getVal(KEY_COLUMN_LIST);
            List<Map<String,Object>> connList = (List<Map<String, Object>>) getVal(KEY_CONNECTION_CONFIG_LIST);
            connection = new ArrayList<>();
            if(connList != null) {
                for(Map<String,Object> conn : connList) {
                    connection.add(new ConnectionConfig(conn));
                }
            }
        }

        public List getColumn() {
            return column;
        }

        public void setColumn(List column) {
            this.column = column;
        }

        public List<ConnectionConfig> getConnection() {
            return connection;
        }

        public void setConnection(List<ConnectionConfig> connection) {
            this.connection = connection;
        }

        public class ConnectionConfig extends AbstractConfig {
            private static final String KEY_JDBC_URL = "jdbcUrl";
            private static final String KEY_TABLE_LIST = "table";
            public static final String KEY_SCHEMA = "schema";

            private String jdbcUrl;
            private List<String> table;
            private String schema;

            public ConnectionConfig(Map<String, Object> map) {
                super(map);
                Object jdbcUrlObj = internalMap.get(KEY_JDBC_URL);
                if(jdbcUrlObj instanceof String){
                    jdbcUrl = jdbcUrlObj.toString();
                } else if(jdbcUrlObj instanceof List && CollectionUtils.isNotEmpty((List) jdbcUrlObj)){
                    jdbcUrl = ((List) jdbcUrlObj).get(0).toString();
                }
                table = (List<String>) getVal(KEY_TABLE_LIST);
                schema = (String) getVal(KEY_SCHEMA);
            }

            public String getJdbcUrl() {
                return jdbcUrl;
            }

            public void setJdbcUrl(String jdbcUrl) {
                this.jdbcUrl = jdbcUrl;
            }

            public List<String> getTable() {
                return table;
            }

            public void setTable(List<String> table) {
                this.table = table;
            }

            public String getSchema(){return schema;}

            public void setSchema(String schema){this.schema = schema;}
        }

    }


}
