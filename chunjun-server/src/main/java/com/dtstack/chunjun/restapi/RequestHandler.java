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
package com.dtstack.chunjun.restapi;

import com.dtstack.chunjun.client.YarnSessionClient;
import com.dtstack.chunjun.entry.JobConverter;
import com.dtstack.chunjun.entry.JobDescriptor;
import com.dtstack.chunjun.entry.JobInfoReq;
import com.dtstack.chunjun.entry.JobLogVO;
import com.dtstack.chunjun.entry.JobSubmitReq;
import com.dtstack.chunjun.entry.JobSubmitVO;
import com.dtstack.chunjun.entry.ResponseValue;
import com.dtstack.chunjun.entry.StatusVO;
import com.dtstack.chunjun.server.SessionManager;
import com.dtstack.chunjun.server.util.JsonMapper;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * @author xuchao
 * @date 2023-09-19
 */
public class RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

    private static final String GET_LOG_URL = "/jobLog";

    private static final String GET_STATUS_URL = "/jobstatus";

    private static final String SUBMIT_JOB_URL = "/submitJob";

    private Javalin app;

    private SessionManager sessionManager;

    public RequestHandler(Javalin app, SessionManager sessionManager) {
        this.app = app;
        this.sessionManager = sessionManager;
    }

    public void register() {
        registerLogAPI();
        registerStatusAPI();
        registerSubmitAPI();
    }

    /** 注册获取指定jobid 状态api */
    public void registerStatusAPI() {
        LOG.info("register get status api");
        app.post(
                GET_STATUS_URL,
                ctx -> {
                    JobInfoReq jobInfoReq = ctx.bodyAsClass(JobInfoReq.class);
                    String jobId = jobInfoReq.getJobId();
                    Preconditions.checkState(!Strings.isNullOrEmpty(jobId), "jobId can't be null");
                    YarnSessionClient yarnSessionClient = sessionManager.getYarnSessionClient();
                    String status = yarnSessionClient.getJobStatus(jobId);

                    StatusVO statusVO = new StatusVO();
                    statusVO.setJobId(jobId);
                    statusVO.setJobStatus(status);

                    ResponseValue responseValue = new ResponseValue();
                    responseValue.setCode(0);
                    responseValue.setData(JsonMapper.writeValueAsString(statusVO));

                    ctx.result(JsonMapper.writeValueAsString(responseValue));
                });
    }

    /** 注册获取jobid 对应日志的api */
    public void registerLogAPI() {
        LOG.info("register get job log api");
        app.post(
                GET_LOG_URL,
                ctx -> {
                    JobInfoReq jobInfoReq = ctx.bodyAsClass(JobInfoReq.class);
                    String jobId = jobInfoReq.getJobId();
                    Preconditions.checkState(!Strings.isNullOrEmpty(jobId), "jobId can't be null");
                    YarnSessionClient yarnSessionClient = sessionManager.getYarnSessionClient();
                    String jobLog = yarnSessionClient.getJobLog(jobId);

                    JobLogVO jobLogVO = new JobLogVO();
                    jobLogVO.setJobId(jobId);
                    jobLogVO.setLog(jobLog);

                    ResponseValue responseValue = new ResponseValue();
                    responseValue.setCode(0);
                    responseValue.setData(JsonMapper.writeValueAsString(jobLogVO));

                    ctx.result(JsonMapper.writeValueAsString(responseValue));
                });
    }

    /** 注册提交任务的api */
    public void registerSubmitAPI() {
        LOG.info("register submit job api");
        app.post(
                SUBMIT_JOB_URL,
                ctx -> {
                    JobSubmitReq jobInfoReq = ctx.bodyAsClass(JobSubmitReq.class);
                    String job = jobInfoReq.getJob();
                    if (jobInfoReq.isURLDecode()) {
                        job = URLDecoder.decode(job, StandardCharsets.UTF_8.toString());
                        jobInfoReq.setJob(job);
                        jobInfoReq.setURLDecode(false);
                    }

                    Preconditions.checkState(!Strings.isNullOrEmpty(job), "jobInfo can't be null");
                    YarnSessionClient yarnSessionClient = sessionManager.getYarnSessionClient();
                    JobDescriptor jobDescriptor = JobConverter.convertReqToJobDescr(jobInfoReq);
                    String jobId = yarnSessionClient.submitJob(jobDescriptor);

                    JobSubmitVO jobLogVO = new JobSubmitVO();
                    jobLogVO.setJobId(jobId);

                    ResponseValue responseValue = new ResponseValue();
                    responseValue.setCode(0);
                    responseValue.setData(JsonMapper.writeValueAsString(jobLogVO));

                    ctx.result(JsonMapper.writeValueAsString(responseValue));
                });
    }
}
