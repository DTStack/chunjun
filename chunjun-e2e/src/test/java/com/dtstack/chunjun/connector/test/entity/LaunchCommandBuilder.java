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

package com.dtstack.chunjun.connector.test.entity;

import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.util.GsonUtil;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LaunchCommandBuilder {
    private List<String> commonds;

    public LaunchCommandBuilder(String jobType) {
        Assert.assertTrue("sync".equals(jobType) || "sql".equals(jobType));
        this.commonds = new ArrayList<>(18);
        commonds.add("-jobType");
        commonds.add(jobType);
    }

    public LaunchCommandBuilder withRunningMode(ClusterMode clusterMode) {
        commonds.add("-mode");
        commonds.add(clusterMode.name());
        return this;
    }

    public LaunchCommandBuilder withJobContentPath(String contentPath) {
        commonds.add("-job");
        commonds.add(contentPath);
        return this;
    }

    public LaunchCommandBuilder withChunJunDistDir(String chunJunDistDir) {
        commonds.add("-chunjunDistDir");
        commonds.add(chunJunDistDir);
        return this;
    }

    public LaunchCommandBuilder withFlinkConfDir(String flinkConfDir) {
        commonds.add("-flinkConfDir");
        commonds.add(flinkConfDir);
        return this;
    }

    public LaunchCommandBuilder withFlinkCustomConf(Map<String, Object> properties) {
        commonds.add("-confProp");
        commonds.add(GsonUtil.GSON.toJson(properties));
        return this;
    }

    public LaunchCommandBuilder withAddJar(List<String> path) {
        commonds.add("-addjar");
        commonds.add(GsonUtil.GSON.toJson(path));
        return this;
    }

    public LaunchCommandBuilder withShipFile(String path) {
        commonds.add("-addShipfile");
        commonds.add(path);
        return this;
    }

    public String[] builder() {
        return commonds.toArray(new String[0]);
    }
}
