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

package com.dtstack.flinkx.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.RowUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static com.dtstack.flinkx.writer.WriteErrorTypes.*;

/**
 * The class handles dirty data management
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class DirtyDataManager {

    private String location;
    private Configuration config;
    private BufferedWriter bw;
    private String[] fieldNames;

    private static final String FIELD_DELIMITER = "\u0001";
    private static final String LINE_DELIMITER = "\n";


    private static List<String> PRIMARY_CONFLICT_KEYWORDS = new ArrayList<>();
    private Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    static {
        PRIMARY_CONFLICT_KEYWORDS.add("duplicate entry");
        PRIMARY_CONFLICT_KEYWORDS.add("unique constraint");
        PRIMARY_CONFLICT_KEYWORDS.add("primary key constraint");
    }

    public DirtyDataManager(String path, Map<String,String> configMap, String[] fieldNames) {
        this.fieldNames = fieldNames;
        location = path + "/" + UUID.randomUUID() + ".txt";
        config = new Configuration();
        if(configMap != null) {
            for(Map.Entry<String,String> entry : configMap.entrySet()) {
                config.set(entry.getKey(), entry.getValue());
            }
        }
        config.set("fs.hdfs.impl.disable.cache", "true");
    }

    public String writeData(Row row, WriteRecordException ex) {
        String content = RowUtil.rowToJson(row, fieldNames);
        String errorType = retrieveCategory(ex);
        String line = StringUtils.join(new String[]{content,errorType, gson.toJson(ex.toString()), DateUtil.timestampToString(new Date()) }, FIELD_DELIMITER);
        try {
            bw.write(line);
            bw.write(LINE_DELIMITER);
            return errorType;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String retrieveCategory(WriteRecordException ex) {
        Throwable cause = ex.getCause();
        if(cause instanceof NullPointerException) {
            return ERR_NULL_POINTER;
        }
        for(String keyword : PRIMARY_CONFLICT_KEYWORDS) {
            if(cause.toString().toLowerCase().contains(keyword)) {
                return ERR_PRIMARY_CONFLICT;
            }
        }
        return ERR_FORMAT_TRANSFORM;
    }

    public void open() {
        try {
            FileSystem fs = FileSystem.get(config);
            Path path = new Path(location);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if(bw != null) {
            try {
                bw.flush();
                bw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
