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

import com.dtstack.chunjun.config.WebConfig;
import com.dtstack.chunjun.entry.ResponseValue;
import com.dtstack.chunjun.server.SessionManager;
import com.dtstack.chunjun.server.util.JsonMapper;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author xuchao
 * @date 2023-09-19
 */
public class WebServer {

    private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);

    private WebConfig webConfig;

    private Javalin app;

    private RequestHandler requestHandler;

    private SessionManager sessionManager;

    public WebServer(WebConfig webConfig, SessionManager sessionManager) {
        this.webConfig = webConfig;
        this.sessionManager = sessionManager;
    }

    public void startServer() {
        int port = webConfig.getPort();
        if (!(port == 0 || (1024 <= port && port < 65536))) {
            throw new IllegalArgumentException(
                    String.format(
                            "startPort should be between 1024 and 65535 (inclusive), "
                                    + "or 0 for a random free port. but now is %s.",
                            port));
        }

        app = Javalin.create().start(port);
        requestHandler = new RequestHandler(app, sessionManager);
        LOG.info("===============start web sever on port {}===========================", port);
        app.get("/", ctx -> ctx.result("Hello ChunJun"));
        requestHandler.register();
        addExceptionHandler();
    }

    public void addExceptionHandler() {
        LOG.info("add exception handler");
        app.exception(
                Exception.class,
                (e, ctx) -> {
                    ResponseValue responseValue = new ResponseValue();
                    responseValue.setCode(1);
                    responseValue.setErrorMsg(e.getMessage());
                    String responseMsg = "";

                    try {
                        responseMsg = JsonMapper.writeValueAsString(responseValue);
                    } catch (IOException ex) {
                        responseMsg = "{\"code\":1, \"errorMsg\":\"" + ex.getMessage() + "\"}";
                        ctx.status(500).result(responseMsg);
                    }
                    ctx.status(500).result(responseMsg);
                });
    }
}
