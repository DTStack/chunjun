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

import com.dtstack.chunjun.connector.ftp.conf.FtpConfig;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author dujie
 * @date 2021-09-23
 */
public interface FileReadClient extends Closeable {

    /**
     * openResource
     *
     * @param inputStream
     * @param ftpConfig
     * @throws IOException
     */
    void open(File file, InputStream inputStream, FtpConfig ftpConfig) throws IOException;

    /**
     * Is there another piece of data
     *
     * @return
     * @throws IOException
     */
    boolean hasNext() throws IOException;

    /**
     * Get the data that has been cut by the fieldDelimiter
     *
     * @return
     * @throws IOException
     */
    String[] nextRecord() throws IOException;
}
