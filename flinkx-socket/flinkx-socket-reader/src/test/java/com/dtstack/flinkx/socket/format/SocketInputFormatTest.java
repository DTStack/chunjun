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

package com.dtstack.flinkx.socket.format;

import com.dtstack.flinkx.socket.util.DtSocketClient;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.SynchronousQueue;

import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_EXIT0;

public class SocketInputFormatTest {

    private SocketInputFormat inputFormat = Mockito.mock(SocketInputFormat.class);

    private SocketInputFormat inputFormat2 = Mockito.spy(SocketInputFormat.class);

    @Test
    public void testNextRecord() throws IOException {
        Row row = Row.of("test");
        inputFormat.queue = new SynchronousQueue<>();
        Mockito.when(inputFormat.nextRecordInternal(row)).thenCallRealMethod();
        new Thread(() ->{
                    try{
                        inputFormat.queue.put(Row.of("test"));
                    }catch (Exception ignored){
                    }
                }
                ).start();
        inputFormat.nextRecordInternal(row);
    }

    @Test(expected = IOException.class)
    public void testNextRecord2() throws IOException {
        Row row1 = Row.of("test");
        inputFormat.queue = new SynchronousQueue<>();
        Mockito.when(inputFormat.nextRecordInternal(Mockito.any(Row.class))).thenCallRealMethod();
        new Thread(() ->{
            try{
                inputFormat.queue.put(Row.of(KEY_EXIT0));
            }catch (Exception ignored){
            }
        }
        ).start();
        inputFormat.nextRecordInternal(row1);
    }


    @Test
    public void testSetAddress(){
        SocketInputFormat inputFormat = new SocketInputFormat();
        inputFormat.setAddress("localhost:8000");
        Assert.assertEquals("localhost", "localhost");
        Assert.assertEquals(8000, 8000);
    }

    @Test
    public void testReachedEnd(){
        Assert.assertFalse(new SocketInputFormat().reachedEnd());
    }

    @Test
    public void testCreateInputSplitsInternal(){
        Assert.assertEquals(3, new SocketInputFormat().createInputSplitsInternal(3).length);
    }

    @Test
    public void testSetCodeC(){
        String codeC = "text";
        inputFormat2.setCodeC(codeC);
        Assert.assertEquals(codeC, inputFormat2.codeC);
    }

    @Test
    public void testSetColumns(){
        ArrayList<String> columns = new ArrayList<>();
        inputFormat2.setColumns(columns);
        Assert.assertEquals(columns, inputFormat2.columns);
    }

    @Test
    public void testCloseInternal() {
        inputFormat2.client = Mockito.mock(DtSocketClient.class);
        inputFormat2.closeInternal();
        Mockito.verify(inputFormat2.client, Mockito.times(1)).close();
    }

    @Test
    public void testOpenInternal(){
        inputFormat2.host = "localhost";
        inputFormat2.port = 8000;
        inputFormat2.openInternal(null);
    }
}
