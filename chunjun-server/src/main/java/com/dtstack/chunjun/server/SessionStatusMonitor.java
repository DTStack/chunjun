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
import com.dtstack.chunjun.http.PoolHttpClient;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 监控对应session的监控状态 基于对jobmanager 获取tm 列表的restapi 进行检查
 *
 * @author xuchao
 * @date 2023-05-17
 */
public class SessionStatusMonitor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SessionStatusMonitor.class);

    private static final String REQ_URL_FORMAT = "%s/jobmanager/config";

    private static final long CHECK_INTERVAL = 5 * 1000L;

    /** 从accepted --> running 额外等待时间 */
    private static final long ACCEPTED_GAP = 10 * 1000L;

    private static final int FAIL_LIMIT = 3;

    private ExecutorService executorService;

    private YarnSessionClient yarnSessionClient;

    private SessionStatusInfo sessionStatusInfo;

    private boolean run = true;

    private int checkFailCount = 0;

    public SessionStatusMonitor(
            YarnSessionClient yarnSessionClient, SessionStatusInfo sessionStatusInfo) {
        this.yarnSessionClient = yarnSessionClient;
        this.sessionStatusInfo = sessionStatusInfo;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void start() {
        executorService.submit(this);
        LOG.info("session status check started!");
    }

    public void shutdown() {
        run = false;
        executorService.shutdown();
        LOG.info("session status check stopped!");
    }

    @Override
    public void run() {

        LOG.warn("session status check thread start!");
        while (run) {
            try {

                // yarnSessionClient 没有获取到对应的appid,不需要进行check 等到重新构建yarnSessionClient 再check
                if (yarnSessionClient.getClient() == null
                        && sessionStatusInfo.getStatus() == ESessionStatus.HEALTHY) {
                    yarnSessionClient.open();
                    continue;
                }

                if (sessionStatusInfo.getStatus() == ESessionStatus.UNHEALTHY) {
                    continue;
                }

                // 检查yarn 对应app的状态，如果是Accepted 的情况,等待
                YarnApplicationState yarnApplicationState =
                        yarnSessionClient.getCurrentSessionStatus();
                if (yarnApplicationState == YarnApplicationState.ACCEPTED) {
                    LOG.warn("current session is Accepted. ");
                    Thread.sleep(ACCEPTED_GAP);
                    continue;
                }

                String reqWebURL =
                        String.format(
                                REQ_URL_FORMAT, yarnSessionClient.getClient().getWebInterfaceURL());
                String response = PoolHttpClient.get(reqWebURL);
                if (response == null) {
                    // 异常情况:连续3次认为session已经不可以用了
                    checkFailCount++;
                } else {
                    checkFailCount = 0;
                }

            } catch (Exception e) {
                checkFailCount++;
            } finally {
                try {
                    Thread.sleep(CHECK_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.warn("", e);
                }
            }

            if (checkFailCount >= FAIL_LIMIT) {
                sessionStatusInfo.setStatus(ESessionStatus.UNHEALTHY);
                try {
                    yarnSessionClient.close();
                } catch (IOException e) {
                    LOG.warn("close yarnSessionClient exception.", e);
                }
                LOG.warn("current check failed.");
            }
        }

        LOG.warn("session status check thread exit!");
    }
}
