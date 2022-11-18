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

import java.util.LinkedList;

public class JobConfBuilder {

    private JobConfig jobConfig;

    public static JobConfBuilder newBuilder() {
        return new JobConfBuilder();
    }

    private JobConfBuilder() {
        this.jobConfig = new JobConfig();
    }

    public JobConfBuilder setting() {
        this.jobConfig.setSetting(new SettingConfig());
        return this;
    }

    public JobConfBuilder setting(SettingConfig settingConfig) {
        this.jobConfig.setSetting(settingConfig);
        return this;
    }

    public JobConfBuilder content(LinkedList<ContentConfig> content) {
        this.jobConfig.setContent(content);
        return this;
    }

    public JobConfig build() {
        return jobConfig;
    }
}
