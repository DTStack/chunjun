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

package com.dtstack.flinkx.socket.util;

import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.SynchronousQueue;

public class DtSocketClientTest {


    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    public DtSocketClient client;

    public static final String HOST = "localhost";

    public static final int PORT = 8000;

    public static final String MESSAGE = "message";

    SynchronousQueue<Row> queue = new SynchronousQueue<>();



    @Before
    public void testDtSocketClient(){
        client = new DtSocketClient(HOST, PORT, queue);
    }

    @Test
    public void testStart() throws InterruptedException {
        new Thread(this::socketServer).start();
        client.start();
        Row row = queue.take();
        Assert.assertEquals(row.getField(0), MESSAGE);
    }

    public void socketServer() {
        try{
            ServerSocket ss = new ServerSocket(PORT);
            Thread.sleep(3000);
            Socket s = ss.accept();
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
            bw.write(MESSAGE);
            bw.flush();
            s.close();
        }catch (Exception e){
            LOG.error("{}", ExceptionUtil.getErrorMessage(e));
        }

    }



}
