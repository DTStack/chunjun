/**
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
package com.dtstack.flinkx.binlog;


import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;


public class BinlogPosUtil {

    private static final Logger logger = LoggerFactory.getLogger(BinlogPosUtil.class);

    private static final Gson gson = new Gson();

    private BinlogPosUtil() {
        // Where did we come from? And why is the universe the way it is?
    }

    public static void savePos(String id, EntryPosition entryPosition) throws IOException {
        try(JsonWriter jsonWriter = new JsonWriter(new PrintWriter(new FileOutputStream(id)))) {
            gson.toJson(entryPosition, EntryPosition.class, jsonWriter);
        }
    }

    public static EntryPosition readPos(String id) throws IOException {
        try(JsonReader jsonReader = new JsonReader(new InputStreamReader(new FileInputStream(id)))) {
            return gson.fromJson(jsonReader, EntryPosition.class);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(readPos("shit.json"));
    }


}
