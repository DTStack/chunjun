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
package com.dtstack.chunjun.config;

import java.io.Serializable;
import java.util.StringJoiner;

public class RestoreConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 是否为实时任务 */
    private boolean isStream = false;
    /** 是否开启断点续传 */
    private boolean isRestore = false;
    /** 断点续传字段名称 */
    private String restoreColumnName;
    /** 断点续传字段类型 */
    private String restoreColumnType;
    /** 断点续传字段索引ID */
    private int restoreColumnIndex = -1;
    /** 触发checkpoint数据条数 */
    private int maxRowNumForCheckpoint = 10000;

    public boolean isStream() {
        return isStream;
    }

    public void setStream(boolean stream) {
        isStream = stream;
    }

    public boolean isRestore() {
        return isRestore;
    }

    public void setRestore(boolean restore) {
        isRestore = restore;
    }

    public String getRestoreColumnName() {
        return restoreColumnName;
    }

    public void setRestoreColumnName(String restoreColumnName) {
        this.restoreColumnName = restoreColumnName;
    }

    public String getRestoreColumnType() {
        return restoreColumnType;
    }

    public void setRestoreColumnType(String restoreColumnType) {
        this.restoreColumnType = restoreColumnType;
    }

    public int getRestoreColumnIndex() {
        return restoreColumnIndex;
    }

    public void setRestoreColumnIndex(int restoreColumnIndex) {
        this.restoreColumnIndex = restoreColumnIndex;
    }

    public int getMaxRowNumForCheckpoint() {
        return maxRowNumForCheckpoint;
    }

    public void setMaxRowNumForCheckpoint(int maxRowNumForCheckpoint) {
        this.maxRowNumForCheckpoint = maxRowNumForCheckpoint;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RestoreConfig.class.getSimpleName() + "[", "]")
                .add("isStream=" + isStream)
                .add("isRestore=" + isRestore)
                .add("restoreColumnName='" + restoreColumnName + "'")
                .add("restoreColumnType='" + restoreColumnType + "'")
                .add("restoreColumnIndex=" + restoreColumnIndex)
                .add("maxRowNumForCheckpoint=" + maxRowNumForCheckpoint)
                .toString();
    }
}
