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

package com.dtstack.chunjun.config;

public class SyncConfBuilder {

    private SyncConfig syncConfig;

    public static SyncConfBuilder newBuilder() {
        return new SyncConfBuilder();
    }

    private SyncConfBuilder() {
        this.syncConfig = new SyncConfig();
    }

    public SyncConfBuilder job(JobConfig jobConfig) {
        this.syncConfig.setJob(jobConfig);
        return this;
    }

    public SyncConfig build() {
        return syncConfig;
    }

    public SyncConfBuilder pluginRoot(String pluginRoot) {
        this.syncConfig.setPluginRoot(pluginRoot);
        return this;
    }
}
