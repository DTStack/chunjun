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

package com.dtstack.chunjun.connector.ftp.client;

import com.dtstack.chunjun.connector.ftp.extend.ftp.FtpParseException;
import com.dtstack.chunjun.connector.ftp.handler.Position;

import lombok.Getter;

/** return from ftpSeqBufferedReader contains line and position */
@Getter
public class Data {
    private String[] data;
    private Position position;
    private FtpParseException exception;

    public Data(String[] data, Position position) {
        this.data = data;
        this.position = position;
    }

    public Data(String[] data, Position position, FtpParseException exception) {
        this.data = data;
        this.position = position;
        this.exception = exception;
    }
}
