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

package com.dtstack.chunjun.connector.ftp.extend.ftp.format;

import com.dtstack.chunjun.connector.ftp.extend.ftp.File;
import com.dtstack.chunjun.connector.ftp.extend.ftp.IFormatConfig;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public interface IFileReadFormat extends Closeable {

    /**
     * Opens a file read format to read the inputStream.
     *
     * @param inputStream the inputStream that need to read.
     * @throws IOException Thrown, if the inputStream could not be opened due to an I/O problem.
     */
    void open(File file, InputStream inputStream, IFormatConfig config) throws IOException;

    /**
     * Method used to check if the end of the input is reached.
     *
     * <p>When this method is called, the read format it guaranteed to be opened.
     *
     * @throws IOException Thrown, if an I/O error occurred. @
     */
    boolean hasNext() throws IOException;

    /**
     * Reads the next record from the input.
     *
     * <p>When this method is called, the read format it guaranteed to be opened.
     *
     * @return Read record.
     * @throws IOException Thrown, if an I/O error occurred.
     */
    String[] nextRecord() throws IOException;

    /**
     * Called after the hasNext method returns false to close the current resource.
     *
     * <p>When this method is called, the read format it guaranteed to be opened.
     *
     * @throws IOException Thrown, if an I/O error occurred.
     */
    void close() throws IOException;
}
