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

package com.dtstack.flinkx.latch;

import com.dtstack.flinkx.util.SysUtil;

/**
 * Latch is a synchronizing Toolkit
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseLatch {

    protected static int MAX_RETRY_TIMES = 30;

    /**
     * 从Flink REST API获取累加器里的值
     * @return 累加器里的值
     */
    public abstract int getVal();

    public void waitUntil(int val) {
        int i = 0;
        for(; i < MAX_RETRY_TIMES; ++i) {
            if(getVal() == val) {
                clear();
                break;
            }
            sleep();
        }

        if(i == MAX_RETRY_TIMES) {
            throw new RuntimeException("Can't wait any longer because max retry times exceeded.");
        }
    }

    protected void sleep() {
        SysUtil.sleep(1000);
    }

    /**
     * 更新累加器里的值
     */
    public abstract void addOne();

    protected void clear() {

    }

}
