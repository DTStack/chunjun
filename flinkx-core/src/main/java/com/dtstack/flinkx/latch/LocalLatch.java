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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Local implementation of Latch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class LocalLatch extends BaseLatch {

    private static Map<String, AtomicInteger> valMap = new ConcurrentHashMap<>();
    private AtomicInteger val;
    private String id;

    public LocalLatch(String id) {
        valMap.putIfAbsent(id, new AtomicInteger());
        this.id = id;
        val = valMap.get(id);
    }

    @Override
    public int getVal() {
        return val.get();
    }

    @Override
    public void addOne() {
        val.incrementAndGet();
    }

    @Override
    protected void clear() {
        valMap.remove(id);
    };

}
