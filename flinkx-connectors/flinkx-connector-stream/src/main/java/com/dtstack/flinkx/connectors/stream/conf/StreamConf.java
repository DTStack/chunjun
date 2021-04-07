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
package com.dtstack.flinkx.connectors.stream.conf;

import com.dtstack.flinkx.conf.FieldConf;

import java.io.Serializable;
import java.util.List;

/**
 * Date: 2021/04/07
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class StreamConf implements Serializable {
    private static final long serialVersionUID = 1L;

    //reader
    private List<Long> sliceRecordCount;
    private List<FieldConf> column;

    //writer
    private boolean print = false;
    private String writeDelimiter = "|";

    public List<Long> getSliceRecordCount() {
        return sliceRecordCount;
    }

    public void setSliceRecordCount(List<Long> sliceRecordCount) {
        this.sliceRecordCount = sliceRecordCount;
    }

    public List<FieldConf> getColumn() {
        return column;
    }

    public void setColumn(List<FieldConf> column) {
        this.column = column;
    }

    public boolean isPrint() {
        return print;
    }

    public void setPrint(boolean print) {
        this.print = print;
    }

    public String getWriteDelimiter() {
        return writeDelimiter;
    }

    public void setWriteDelimiter(String writeDelimiter) {
        this.writeDelimiter = writeDelimiter;
    }

    @Override
    public String toString() {
        return "StreamConf{" +
                "sliceRecordCount=" + sliceRecordCount +
                ", column=" + column +
                ", print=" + print +
                ", writeDelimiter='" + writeDelimiter + '\'' +
                '}';
    }
}
