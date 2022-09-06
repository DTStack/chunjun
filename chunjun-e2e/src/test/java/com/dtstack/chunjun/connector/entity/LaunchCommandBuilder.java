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

package com.dtstack.chunjun.connector.entity;

import com.dtstack.chunjun.enums.ClusterMode;

import com.google.gson.GsonBuilder;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LaunchCommandBuilder {
    private List<String> commands;

    public LaunchCommandBuilder(String jobType) {
        Assert.assertTrue("sync".equals(jobType) || "sql".equals(jobType));
        this.commands = new ArrayList<>(18);
        commands.add("-jobType");
        commands.add(jobType);
    }

    public LaunchCommandBuilder withRunningMode(ClusterMode clusterMode) {
        commands.add("-mode");
        commands.add(clusterMode.name());
        return this;
    }

    public LaunchCommandBuilder withJobContentPath(String contentPath) {
        commands.add("-job");
        commands.add(contentPath);
        return this;
    }

    public LaunchCommandBuilder withChunJunDistDir(String chunJunDistDir) {
        commands.add("-chunjunDistDir");
        commands.add(chunJunDistDir);
        return this;
    }

    public LaunchCommandBuilder withFlinkLibDir(String chunJunDistDir) {
        commands.add("-flinkLibDir");
        commands.add(chunJunDistDir);
        return this;
    }

    public LaunchCommandBuilder withFlinkConfDir(String flinkConfDir) {
        commands.add("-flinkConfDir");
        commands.add(flinkConfDir);
        return this;
    }

    public LaunchCommandBuilder withFlinkCustomConf(Map<String, Object> properties) {
        commands.add("-confProp");
        commands.add(new GsonBuilder().create().toJson(properties));
        return this;
    }

    public LaunchCommandBuilder withAddJar(List<String> path) {
        commands.add("-addjar");
        commands.add(new GsonBuilder().create().toJson(path));
        return this;
    }

    public LaunchCommandBuilder withShipFile(String path) {
        commands.add("-addShipfile");
        commands.add(path);
        return this;
    }

    public LaunchCommandBuilder withParameters(Map<String, String> parameters) {
        if (parameters != null && !parameters.isEmpty()) {
            commands.add("-p");
            String params =
                    parameters.entrySet().stream()
                            .map(entry -> entry.getKey() + "=" + entry.getValue())
                            .collect(Collectors.joining(","));
            commands.add(params);
        }
        return this;
    }

    public String[] builder() {
        return commands.toArray(new String[0]);
    }
}
