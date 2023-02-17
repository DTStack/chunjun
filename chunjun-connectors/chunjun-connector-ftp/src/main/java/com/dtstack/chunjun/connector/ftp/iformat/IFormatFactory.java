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

package com.dtstack.chunjun.connector.ftp.iformat;

import com.dtstack.chunjun.connector.ftp.enums.FileType;
import com.dtstack.chunjun.connector.ftp.extend.ftp.format.IFileReadFormat;

public class IFormatFactory {

    public static IFileReadFormat create(FileType fileType, String className) {
        switch (fileType) {
            case CSV:
                return new CsvFileFormat();
            case EXCEL:
                return new ExcelFileFormat();
            case CUSTOM:
                return createCustom(className);
            default:
                return new TextFileFormat();
        }
    }

    public static IFileReadFormat createCustom(String className) {
        Class<?> userClass;
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            userClass = classLoader.loadClass(className);
            return (IFileReadFormat) userClass.newInstance();
        } catch (Exception e) {
            String errorMessage =
                    String.format("Create custom format failed, class name : %s.", className);
            throw new RuntimeException(errorMessage, e);
        }
    }
}
