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
package com.dtstack.chunjun.conf;

import com.dtstack.chunjun.constants.ConfigConstant;

import java.io.Serializable;

/**
 * Date: 2021/01/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class RestartConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** flink失败重试策略 */
    private String strategy = ConfigConstant.STRATEGY_NO_RESTART;
    /** FixedDelayRestartStrategy策略下的重新启动尝试次数 */
    private int restartAttempts = 3;
    /** 重试延迟时间，单位秒 */
    private int delayInterval = 10;
    /** 作业失败之前，给定间隔中的最大重新启动次数 */
    private int failureRate = 2;
    /** 两次重启之间的延迟时间，单位秒 */
    private int failureInterval = 60;

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public int getRestartAttempts() {
        return restartAttempts;
    }

    public void setRestartAttempts(int restartAttempts) {
        this.restartAttempts = restartAttempts;
    }

    public int getDelayInterval() {
        return delayInterval;
    }

    public void setDelayInterval(int delayInterval) {
        this.delayInterval = delayInterval;
    }

    public int getFailureRate() {
        return failureRate;
    }

    public void setFailureRate(int failureRate) {
        this.failureRate = failureRate;
    }

    public int getFailureInterval() {
        return failureInterval;
    }

    public void setFailureInterval(int failureInterval) {
        this.failureInterval = failureInterval;
    }

    @Override
    public String toString() {
        return "RestartConf{"
                + "strategy='"
                + strategy
                + '\''
                + ", restartAttempts="
                + restartAttempts
                + ", delayInterval="
                + delayInterval
                + ", failureRate="
                + failureRate
                + ", failureInterval="
                + failureInterval
                + '}';
    }
}
