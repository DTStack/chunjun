/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadata.core.entity;

import java.io.Serializable;

/**
 * @author kunni@dtstack.com
 */
public class MetadataEntity implements Serializable {

    protected static final long serialVersionUID = 1L;

    /**任务返回体schema*/
    protected String schema;

    /**当前对象是否同步成功*/
    protected boolean querySuccess;

    /**失败报错信息*/
    protected String errorMsg;

    /**操作类型*/
    protected String operaType;

    public void setOperaType(String operaType) {
        this.operaType = operaType;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setQuerySuccess(boolean querySuccess) {
        this.querySuccess = querySuccess;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @Override
    public String toString() {
        return "MetadataEntity{" +
                "schema='" + schema + '\'' +
                ", querySuccess=" + querySuccess +
                ", errorMsg='" + errorMsg + '\'' +
                ", operaType='" + operaType + '\'' +
                '}';
    }
}
