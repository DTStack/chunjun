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
package com.dtstack.chunjun.entry;

import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuchao
 * @date 2023-09-21
 */
public class JobConverter {

    public static String[] convertJobToArgs(JobDescriptor jobDescriptor)
            throws IllegalAccessException {
        List<String> argList = new ArrayList<>();
        Field[] fields = jobDescriptor.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String val = (String) field.get(jobDescriptor);
            argList.add("-" + field.getName());
            argList.add(val);
        }

        return argList.toArray(new String[argList.size()]);
    }

    /**
     * @param jobSubmitReq
     * @return
     */
    public static JobDescriptor convertReqToJobDescr(JobSubmitReq jobSubmitReq) {
        JobDescriptor jobDescriptor = new JobDescriptor();
        // 当前系统提供的server 模式只支持yarn session 模式下的数据同步场景
        jobDescriptor.setJobType("sync");
        jobDescriptor.setMode("yarn-session");

        String job =
                jobSubmitReq.isURLDecode()
                        ? jobSubmitReq.getJob()
                        : URLEncoder.encode(jobSubmitReq.getJob());
        jobDescriptor.setJob(job);
        jobDescriptor.setConfProp(jobSubmitReq.getConfProp());
        jobDescriptor.setJobName(jobSubmitReq.getJobName());

        return jobDescriptor;
    }
}
