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

package com.dtstack.flinkx.enums;

import org.apache.commons.lang3.StringUtils;

/**
 * This class defines three running mode of FlinkX
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public enum ClusterMode {

    /** Applications executed in the local */
    local(0, "local"),

    /** Applications executed in the standalone */
    standalone(1, "standalone"),

    /** Applications executed in the yarn session */
    yarnSession(2, "yarn-session"),

    /** Applications executed in the yarn perjob */
    yarnPerJob(3, "yarn-per-job"),

    /** Applications executed in the yarn application */
    yarnApplication(4, "yarn-application"),

    /** Applications executed in the kubernetes session */
    kubernetesSession(5, "kubernetes-session"),

    /** Applications executed in the kubernetes perjob */
    kubernetesPerJob(6, "kubernetes-per-job"),

    /** Applications executed in the kubernetes application */
    kubernetesApplication(7, "kubernetes-application");

    private int type;

    private String name;

    ClusterMode(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public static ClusterMode getByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("ClusterMode name cannot be null or empty");
        }
        switch (name) {
            case "standalone":
                return standalone;
            case "yarn":
            case "yarn-session":
                return yarnSession;
            case "yarnPer":
            case "yarn-per-job":
                return yarnPerJob;
            case "yarn-application":
                return yarnApplication;
            case "kubernetes-session":
                return kubernetesSession;
            case "kubernetes-per-job":
                return kubernetesPerJob;
            case "kubernetes-application":
                return kubernetesApplication;
            default:
                return local;
        }
    }
}
