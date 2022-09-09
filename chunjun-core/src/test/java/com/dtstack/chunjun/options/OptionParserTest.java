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

package com.dtstack.chunjun.options;

import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class OptionParserTest {

    @TempDir File tempDir;

    private static final String JOB_FILE = "stream.json";

    private static final String JOB_CONTEXT =
            "{\n"
                    + "  \"job\": {\n"
                    + "    \"content\": [\n"
                    + "      {\n"
                    + "        \"reader\": {\n"
                    + "          \"parameter\": {\n"
                    + "            \"column\": [\n"
                    + "              {\n"
                    + "                \"name\": \"id\",\n"
                    + "                \"type\": \"id\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"name\",\n"
                    + "                \"type\": \"string\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"content\",\n"
                    + "                \"type\": \"string\"\n"
                    + "              }\n"
                    + "            ],\n"
                    + "            \"sliceRecordCount\": [\"30\"],\n"
                    + "            \"permitsPerSecond\": 1\n"
                    + "          },\n"
                    + "          \"table\": {\n"
                    + "            \"tableName\": \"sourceTable\"\n"
                    + "          },\n"
                    + "          \"name\": \"streamreader\"\n"
                    + "        },\n"
                    + "        \"writer\": {\n"
                    + "          \"parameter\": {\n"
                    + "            \"column\": [\n"
                    + "              {\n"
                    + "                \"name\": \"id\",\n"
                    + "                \"type\": \"id\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"name\",\n"
                    + "                \"type\": \"string\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"content\",\n"
                    + "                \"type\": \"timestamp\"\n"
                    + "              }\n"
                    + "            ],\n"
                    + "            \"print\": true\n"
                    + "          },\n"
                    + "          \"table\": {\n"
                    + "            \"tableName\": \"sinkTable\"\n"
                    + "          },\n"
                    + "          \"name\": \"streamwriter\"\n"
                    + "        },\n"
                    + "        \"transformer\": {\n"
                    + "          \"transformSql\": \"select id,name, NOW() from sourceTable where CHAR_LENGTH(name) < 50 and CHAR_LENGTH(content) < 50\"\n"
                    + "        }\n"
                    + "      }\n"
                    + "    ],\n"
                    + "    \"setting\": {\n"
                    + "      \"errorLimit\": {\n"
                    + "        \"record\": 100\n"
                    + "      },\n"
                    + "      \"speed\": {\n"
                    + "        \"bytes\": 0,\n"
                    + "        \"channel\": 1,\n"
                    + "        \"readerChannel\": 1,\n"
                    + "        \"writerChannel\": 1\n"
                    + "      }\n"
                    + "    }\n"
                    + "  }\n"
                    + "}\n";

    @Test
    void getProgramExeArgList() throws Exception {
        File jobFile = createJobFile();
        ImmutableList<String> argList =
                ImmutableList.<String>builder()
                        .add("-" + OptionParser.OPTION_JOB)
                        .add(jobFile.getPath())
                        .build();
        String[] args = argList.toArray(new String[0]);
        OptionParser optionParser = new OptionParser(args);
        String job = URLEncoder.encode(JOB_CONTEXT, StandardCharsets.UTF_8.name());
        assertTrue(optionParser.getProgramExeArgList().contains(job));
    }

    private File createJobFile() throws IOException {
        File jsonFile = Paths.get(tempDir.toString(), JOB_FILE).toFile();
        jsonFile.createNewFile();
        FileUtils.writeFileUtf8(jsonFile, JOB_CONTEXT);
        return jsonFile;
    }
}
