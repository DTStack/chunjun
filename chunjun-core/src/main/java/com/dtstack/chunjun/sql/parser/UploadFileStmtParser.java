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

package com.dtstack.chunjun.sql.parser;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.URL;
import java.util.List;
import java.util.regex.Pattern;

public class UploadFileStmtParser extends AbstractStmtParser {

    private static final Pattern ADD_FILE_AND_JAR_PATTERN =
            Pattern.compile("(?i).*add\\s+file\\s+.+|(?i).*add\\s+jar\\s+.+");

    @Override
    public boolean canHandle(String stmt) {
        return ADD_FILE_AND_JAR_PATTERN.matcher(stmt).find();
    }

    @Override
    public void execStmt(
            String stmt,
            StreamTableEnvironment tEnv,
            StatementSet statementSet,
            List<URL> jarUrlList) {
        // do nothing.
    }
}
