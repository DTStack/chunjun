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
package com.dtstack.chunjun.client;

import com.dtstack.chunjun.entry.JobDescriptor;

import java.io.IOException;

/**
 * 定义操作远程的接口
 *
 * @author xuchao
 * @date 2023-05-22
 */
public interface IClient {

    /** 关联远程信息，获取 */
    void open();

    /** 关闭连接 */
    void close() throws IOException;

    /**
     * 获取任务日志
     *
     * @return
     */
    String getJobLog(String jobId);

    /**
     * 获取任务状态
     *
     * @return
     */
    String getJobStatus(String jobId) throws Exception;

    /**
     * 获取任务统计信息
     *
     * @return
     */
    String getJobStatics(String jobId);

    /** 提交任务 */
    String submitJob(JobDescriptor jobDescriptor) throws Exception;

    /** 取消任务 */
    void cancelJob(String jobId);
}
