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

package com.dtstack.chunjun.connector.ftp.handler;

import com.dtstack.chunjun.connector.ftp.source.FtpFileSplit;

import java.io.Serializable;
import java.util.StringJoiner;

public class Position implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 第几条数据* */
    private final Long currentReadPosition;
    /** 读取的数据文件* */
    private final FtpFileSplit fileSplit;

    public Position(Long currentReadPosition, FtpFileSplit fileSplit) {
        this.currentReadPosition = currentReadPosition;
        this.fileSplit = fileSplit;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Long getCurrentReadPosition() {
        return currentReadPosition;
    }

    public FtpFileSplit getFileSplit() {
        return fileSplit;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Position.class.getSimpleName() + "[", "]")
                .add("currentReadPosition=" + currentReadPosition)
                .add("fileSplit=" + fileSplit)
                .toString();
    }
}
