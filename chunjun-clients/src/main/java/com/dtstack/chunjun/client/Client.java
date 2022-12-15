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

import com.dtstack.chunjun.options.OptionParser;
import com.dtstack.chunjun.options.Options;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Client {

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws Exception {
        // 1. parse client args
        OptionParser optionParser = new OptionParser(args);
        Options launcherOptions = optionParser.getOptions();
        List<String> argList = optionParser.getProgramExeArgList();
    }
}
