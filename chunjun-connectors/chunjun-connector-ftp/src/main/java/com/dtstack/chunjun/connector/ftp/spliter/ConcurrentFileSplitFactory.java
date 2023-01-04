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

package com.dtstack.chunjun.connector.ftp.spliter;

import com.dtstack.chunjun.connector.ftp.config.FtpConfig;
import com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent.ConcurrentFileSplit;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.util.Locale;

public class ConcurrentFileSplitFactory {

    public static ConcurrentFileSplit createConcurrentFileSplit(FtpConfig config) {
        // user defined
        String customSplitClassName = config.getCustomConcurrentFileSplitClassName();
        if (StringUtils.isNotBlank(customSplitClassName)) {
            try {
                Class<?> clazz = Class.forName(customSplitClassName);
                Constructor<?> constructor = clazz.getConstructor();
                return (ConcurrentFileSplit) constructor.newInstance();
            } catch (Exception e) {
                throw new ChunJunRuntimeException(e);
            }
        }

        /* compress file */
        String compressType = config.getCompressType();
        if (StringUtils.isNotBlank(compressType)) {
            if (compressType.toUpperCase(Locale.ENGLISH).equals("ZIP")) {
                return new ConcurrentZipCompressSplit();
            } else {
                throw new ChunJunRuntimeException("not support compress type");
            }
        }

        /* normal file, csv, excel, txt */
        String fileType = config.getFileType();
        switch (fileType.toUpperCase(Locale.ENGLISH)) {
            case "CSV":
            case "TXT":
                return new ConcurrentCsvSplit();

            case "EXCEL":
            default:
                return new DefaultFileSplit();
        }
    }
}
