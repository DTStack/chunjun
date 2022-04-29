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
package com.dtstack.chunjun.connector.stream.conf;

import com.dtstack.chunjun.conf.FlinkxCommonConf;

import java.util.List;

/**
 * Date: 2021/04/07 Company: www.dtstack.com
 *
 * @author tudou 这里是Stream公共的参数
 */
public class StreamConf extends FlinkxCommonConf {

    // reader
    private List<Long> sliceRecordCount;

    // writer
    private boolean print = true;

    private long permitsPerSecond = 0;

    public List<Long> getSliceRecordCount() {
        return sliceRecordCount;
    }

    public void setSliceRecordCount(List<Long> sliceRecordCount) {
        this.sliceRecordCount = sliceRecordCount;
    }

    public boolean getPrint() {
        return print;
    }

    public void setPrint(boolean print) {
        this.print = print;
    }

    public long getPermitsPerSecond() {
        return permitsPerSecond;
    }

    public void setPermitsPerSecond(long permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
    }

    @Override
    public String toString() {
        return "StreamConf{"
                + "sliceRecordCount="
                + sliceRecordCount
                + ", print="
                + print
                + ", permitsPerSecond="
                + permitsPerSecond
                + '}';
    }
}
