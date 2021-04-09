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

package com.dtstack.flinkx.connector.stream.conf;

/**
 * @author chuixue
 * @create 2021-04-09 10:08
 * @description 这里是StreamSink特有的参数
 **/
public class StreamSinkConf extends StreamConf {
    private String printIdentifier;
    private boolean stdErr;

    public String getPrintIdentifier() {
        return printIdentifier;
    }

    public StreamSinkConf setPrintIdentifier(String printIdentifier) {
        this.printIdentifier = printIdentifier;
        return this;
    }

    public boolean getStdErr() {
        return stdErr;
    }

    public StreamSinkConf setStdErr(boolean stdErr) {
        this.stdErr = stdErr;
        return this;
    }

    public static StreamSinkConf builder() {
        return new StreamSinkConf();
    }

    @Override
    public String toString() {
        return "StreamSinkConf{" +
                ", printIdentifier='" + printIdentifier + '\'' +
                ", stdErr=" + stdErr +
                '}';
    }
}
