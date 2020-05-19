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


package com.dtstack.flinkx.outputformat;

import org.apache.commons.lang.StringUtils;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

/**
 * @author jiangbo
 * @date 2019/8/28
 */
public class FileOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    protected BaseFileOutputFormat format;

    public void setFormat(BaseFileOutputFormat format) {
        this.format = format;
        super.format = format;
    }

    public void setWriteMode(String writeMode) {
        this.format.writeMode = StringUtils.isBlank(writeMode) ? "APPEND" : writeMode.toUpperCase();
    }

    public void setPath(String path) {
        this.format.path = path;
    }

    public void setFileName(String fileName) {
        format.fileName = fileName;
    }

    public void setCompress(String compress) {
        format.compress = compress;
    }

    public void setCharSetName(String charsetName) {
        if(StringUtils.isNotEmpty(charsetName)) {
            if(!Charset.isSupported(charsetName)) {
                throw new UnsupportedCharsetException("The charset " + charsetName + " is not supported.");
            }
            this.format.charsetName = charsetName;
        }
    }

    public void setMaxFileSize(long maxFileSize){
        format.maxFileSize = maxFileSize;
    }

    public void setFlushBlockInterval(long interval){
        format.flushInterval = interval;
    }

    @Override
    protected void checkFormat() {
        if (format.path == null || format.path.length() == 0) {
            throw new IllegalArgumentException("No path supplied.");
        }
    }
}
