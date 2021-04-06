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

package com.dtstack.flinkx.exec;


import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 * 解析传递的参数信息
 * Date: 2020/2/24
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class ParamsInfo {

    private final String sql;
    private final String name;
    private final List<URL> jarUrlList;
    private final String localSqlPluginPath;
    private final String remoteSqlPluginPath;
    private final String pluginLoadMode;
    private final String deployMode;
    private final Properties confProp;

    public ParamsInfo(
            String sql,
            String name,
            List<URL> jarUrlList,
            String localSqlPluginPath,
            String remoteSqlPluginPath,
            String pluginLoadMode,
            String deployMode,
            Properties confProp
    ) {

        this.sql = sql;
        this.name = name;
        this.jarUrlList = jarUrlList;
        this.localSqlPluginPath = localSqlPluginPath;
        this.remoteSqlPluginPath = remoteSqlPluginPath;
        this.pluginLoadMode = pluginLoadMode;
        this.deployMode = deployMode;
        this.confProp = confProp;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSql() {
        return sql;
    }

    public String getName() {
        return name;
    }

    public List<URL> getJarUrlList() {
        return jarUrlList;
    }

    public String getLocalSqlPluginPath() {
        return localSqlPluginPath;
    }

    public String getRemoteSqlPluginPath() {
        return remoteSqlPluginPath;
    }

    public String getPluginLoadMode() {
        return pluginLoadMode;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public Properties getConfProp() {
        return confProp;
    }

    @Override
    public String toString() {
        return "ParamsInfo{" +
                "sql='" + sql + '\'' +
                ", name='" + name + '\'' +
                ", jarUrlList=" + jarUrlList +
                ", localSqlPluginPath='" + localSqlPluginPath + '\'' +
                ", remoteSqlPluginPath='" + remoteSqlPluginPath + '\'' +
                ", pluginLoadMode='" + pluginLoadMode + '\'' +
                ", deployMode='" + deployMode + '\'' +
                ", confProp=" + confProp +
                '}';
    }

    public static class Builder {

        private String sql;
        private String name;
        private List<URL> jarUrlList;
        private String localSqlPluginPath;
        private String remoteSqlPluginPath;
        private String pluginLoadMode;
        private String deployMode;
        private Properties confProp;

        public Builder setSql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setJarUrlList(List<URL> jarUrlList) {
            this.jarUrlList = jarUrlList;
            return this;
        }

        public Builder setLocalSqlPluginPath(String localSqlPluginPath) {
            this.localSqlPluginPath = localSqlPluginPath;
            return this;
        }

        public Builder setRemoteSqlPluginPath(String remoteSqlPluginPath) {
            this.remoteSqlPluginPath = remoteSqlPluginPath;
            return this;
        }

        public Builder setPluginLoadMode(String pluginLoadMode) {
            this.pluginLoadMode = pluginLoadMode;
            return this;
        }

        public Builder setDeployMode(String deployMode) {
            this.deployMode = deployMode;
            return this;
        }


        public Builder setConfProp(Properties confProp) {
            this.confProp = confProp;
            return this;
        }

        public ParamsInfo build() {
            return new ParamsInfo(
                    sql,
                    name,
                    jarUrlList,
                    localSqlPluginPath,
                    remoteSqlPluginPath,
                    pluginLoadMode,
                    deployMode,
                    confProp
            );
        }
    }
}
