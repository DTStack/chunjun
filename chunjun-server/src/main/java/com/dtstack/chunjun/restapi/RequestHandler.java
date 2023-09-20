package com.dtstack.chunjun.restapi;

import com.dtstack.chunjun.client.YarnSessionClient;
import com.dtstack.chunjun.entry.StatusReq;
import com.dtstack.chunjun.server.SessionManager;

import io.javalin.Javalin;
import org.apache.flink.shaded.guava30.com.google.common.base.Strings;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-09-19
 */
public class RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

    private static final String GET_LOG_URL = "";

    private static final String GET_STATUS_URL = "/jobstatus";

    private static final String SUBMIT_JOB_URL = "";

    private Javalin app;

    private SessionManager sessionManager;

    public RequestHandler(Javalin app, SessionManager sessionManager){
        this.app = app;
        this.sessionManager =  sessionManager;
    }

    public void register(){
        registerLogAPI();
        registerStatusAPI();
        registerSubmitAPI();
    }

    /**
     * 注册获取指定jobid 状态api
     */
    public void registerStatusAPI(){
        LOG.info("register get status api");
        app.post(GET_STATUS_URL, ctx -> {
            StatusReq statusReq = ctx.bodyAsClass(StatusReq.class);
            String jobId = statusReq.getJobId();
            Preconditions.checkState(Strings.isNullOrEmpty(jobId));
            YarnSessionClient yarnSessionClient = sessionManager.getYarnSessionClient();
            yarnSessionClient.getJobStatus(jobId);
            ctx.result("get status api");

        } );
    }

    /**
     * 注册获取jobid 对应日志的api
     */
    public void registerLogAPI(){

    }

    /**
     * 注册提交任务的api
     */
    public void registerSubmitAPI(){

    }
}
