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

package com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent;

import com.dtstack.chunjun.connector.ftp.extend.ftp.IFormatConfig;
import com.dtstack.chunjun.connector.ftp.extend.ftp.IFtpHandler;

import java.util.Comparator;
import java.util.List;

public interface ConcurrentFileSplit {

    /**
     * 切割文件， 生成 FtpFileSplit
     *
     * @param handler
     * @param config
     * @param files
     * @return
     */
    List<FtpFileSplit> buildFtpFileSplit(
            IFtpHandler handler, IFormatConfig config, List<String> files);

    /**
     * 断点续传时候需要遍历FtpFileSplit，去除已经读过的文件, 所以需要保证FtpFileSplit的顺序;
     *
     * @return
     */
    Comparator<FtpFileSplit> compare();
}
