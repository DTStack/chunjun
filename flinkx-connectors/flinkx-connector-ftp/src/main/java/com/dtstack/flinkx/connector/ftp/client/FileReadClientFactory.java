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

package com.dtstack.flinkx.connector.ftp.client;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dujie
 * @date 2021-09-23
 */
public class FileReadClientFactory {
    private static Logger LOG = LoggerFactory.getLogger(FileReadClientFactory.class);

    public static FileReadClient create(String fileName, String defaultType) {

        String fileType = getFileType(fileName, defaultType);

        switch (FileType.fromString(fileType)) {
            case CSV:
                return new CsvFileReadClient();
            case EXCEL:
                return new ExcelFileReadClient();
            default:
                return new TextFileReadClient();
        }
    }

    private static String getFileType(String fileName, String defaultType) {
        String fileType = "";
        if (StringUtils.isNotBlank(defaultType)) {
            fileType = defaultType;
        } else {
            int i = fileName.lastIndexOf(".");
            if (i != -1 && i != fileName.length() - 1) {
                fileType = fileName.substring(i + 1);
            }
        }

        LOG.info("The file [{}]  extension is {}  ", fileName, fileType);

        return fileType;
    }
}
