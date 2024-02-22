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
package com.dtstack.chunjun.server;

import com.dtstack.chunjun.client.YarnSessionClient;
import com.dtstack.chunjun.config.SessionConfig;
import com.dtstack.chunjun.config.YarnAppConfig;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * session 管理器,对session 进行监控和自管理(根据检查结果启动session)
 *
 * @author xuchao
 * @date 2023-05-16
 */
public class SessionManager {

    private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);

    private static final int DEPLOY_CHECK_INTERVAL = 2 * 1000;

    private static final int DEPLOY_MAX_RETRY = 3;

    private int retryNum = 0;

    private static final String FLINK_VERSION = "flink116";

    private SessionConfig sessionConfig;

    private YarnAppConfig yarnAppConfig;

    private SessionStatusMonitor sessionStatusMonitor;

    private ScheduledExecutorService sessionDeployScheduler;

    private YarnSessionClient yarnSessionClient;

    private final SessionStatusInfo sessionStatusInfo = new SessionStatusInfo();

    private SessionDeployer sessionDeployer;

    public SessionManager(SessionConfig sessionConfig) {
        this.sessionConfig = sessionConfig;
        this.yarnAppConfig = sessionConfig.getAppConfig();
        initYarnSessionClient();
    }

    public void initYarnSessionClient() {
        yarnSessionClient = new YarnSessionClient(sessionConfig);
        yarnSessionClient.open();

        if (yarnSessionClient.getClient() == null) {
            // 初始化的时候检查远程是否存在匹配的session
            sessionStatusInfo.setStatus(ESessionStatus.UNHEALTHY);
        } else {
            sessionStatusInfo.setAppId(yarnSessionClient.getClient().getClusterId().toString());
        }

        sessionDeployScheduler =
                new ScheduledThreadPoolExecutor(
                        1,
                        new ChunJunThreadFactory(
                                "session_deploy_factory",
                                true,
                                (t, e) -> {
                                    LOG.error("session_deploy_factory occur error!", e);
                                }));
    }

    /** 开启session 监控 */
    public void startSessionCheck() {
        sessionStatusMonitor = new SessionStatusMonitor(yarnSessionClient, sessionStatusInfo);
        sessionStatusMonitor.start();
    }

    public void stopSessionCheck() {
        if (sessionStatusMonitor != null) {
            sessionStatusMonitor.shutdown();
            LOG.info("stopSessionCheck stopped");
        }
    }

    public void startSessionDeploy() {

        sessionDeployer = new SessionDeployer(sessionConfig, sessionStatusInfo);
        sessionDeployScheduler.scheduleWithFixedDelay(
                () -> {
                    if (!ESessionStatus.UNHEALTHY.equals(sessionStatusInfo.getStatus())) {
                        return;
                    }

                    LOG.warn("current session status is unhealthy, will deploy a new session.");

                    try {
                        sessionDeployer.doDeploy();
                        LOG.info("deploy yarn session success.");
                        retryNum = 0;
                    } catch (Exception e) {
                        retryNum++;
                        // 每次重试间隔递增retryNum * 30s
                        long sleepTime = retryNum * 30000L;
                        LOG.error(
                                "deploy session error, retryNum:{}, and sleep {}ms",
                                retryNum,
                                sleepTime);
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException ex) {
                            LOG.info("", ex);
                        }

                        if (retryNum >= DEPLOY_MAX_RETRY) {
                            LOG.error(
                                    "!!!!!deploy session retry max then {}, exit schedule thread.",
                                    DEPLOY_MAX_RETRY);
                            throw e;
                        }
                    }
                },
                DEPLOY_CHECK_INTERVAL,
                DEPLOY_CHECK_INTERVAL,
                TimeUnit.MILLISECONDS);
    }

    public void stopSessionDeploy() {
        if (sessionDeployScheduler != null) {
            sessionDeployScheduler.shutdown();
            LOG.info("sessionDeployScheduler stopped");
        }
    }

    public YarnSessionClient getYarnSessionClient() {
        return yarnSessionClient;
    }
}
