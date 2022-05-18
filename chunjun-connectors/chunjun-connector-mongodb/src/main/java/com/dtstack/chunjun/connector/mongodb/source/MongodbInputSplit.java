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

package com.dtstack.chunjun.connector.mongodb.source;

import org.apache.flink.core.io.InputSplit;

/**
 * @author Ada Wong
 * @program chunjun
 * @create 2021/06/24
 */
public class MongodbInputSplit implements InputSplit {

    private int skip;

    private int limit;

    public MongodbInputSplit(int skip, int limit) {
        this.skip = skip;
        this.limit = limit;
    }

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getSplitNumber() {
        return 0;
    }

    @Override
    public String toString() {
        return "MongodbInputSplit{" + "skip=" + skip + ", limit=" + limit + '}';
    }
}
