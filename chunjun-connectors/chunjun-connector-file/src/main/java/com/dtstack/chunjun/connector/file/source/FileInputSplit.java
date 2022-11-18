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

package com.dtstack.chunjun.connector.file.source;

import org.apache.flink.core.io.InputSplit;

import java.util.ArrayList;
import java.util.List;

public class FileInputSplit implements InputSplit {

    private static final long serialVersionUID = -7369448840361207579L;

    private final int splitNumber;

    private List<String> paths = new ArrayList<>();

    public FileInputSplit(int splitNumber) {
        this.splitNumber = splitNumber;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }
}
