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
package com.dtstack.flinkx.restapi.inputformat;

import com.dtstack.flinkx.restapi.common.RestContext;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * httpClient
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/25
 */
public class HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private Thread workThread;
    private BlockingQueue<Row> queue;
    private RestContext restContext;

    public HttpClient() {

    }

    public void start() {
        this.workThread = new Thread(() -> {

        });

        workThread.start();
    }

    public Row takeEvent() {
        Row row = null;
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return row;
    }

}
