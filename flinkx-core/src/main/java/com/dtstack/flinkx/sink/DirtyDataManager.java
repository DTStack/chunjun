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

package com.dtstack.flinkx.sink;

import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.RowUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.dtstack.flinkx.sink.WriteErrorTypes.ERR_FORMAT_TRANSFORM;
import static com.dtstack.flinkx.sink.WriteErrorTypes.ERR_NULL_POINTER;
import static com.dtstack.flinkx.sink.WriteErrorTypes.ERR_PRIMARY_CONFLICT;

/**
 * The class handles dirty data management
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class DirtyDataManager {

    private static final List<String> PRIMARY_CONFLICT_KEYWORDS = new ArrayList<>();
    private final String location;
    private final Map<String, Object> config;
    private final String[] fieldNames;
    private final String jobId;
    private FSDataOutputStream stream;
    private final DistributedCache distributedCache;

    private static final String FIELD_DELIMITER = "\u0001";
    private static final String LINE_DELIMITER = "\n";
    private final EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags =
            EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH);
    private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    static {
        PRIMARY_CONFLICT_KEYWORDS.add("duplicate entry");
        PRIMARY_CONFLICT_KEYWORDS.add("unique constraint");
        PRIMARY_CONFLICT_KEYWORDS.add("primary key constraint");
    }

    public DirtyDataManager(
            String path,
            Map<String, Object> configMap,
            String[] fieldNames,
            String jobId,
            DistributedCache distributedCache) {
        this.fieldNames = fieldNames;
        location = path + "/" + UUID.randomUUID() + ".txt";
        this.config = configMap;
        this.jobId = jobId;
        this.distributedCache = distributedCache;
    }

    public String writeData(RowData rowData, WriteRecordException ex) {
        String content = RowUtil.rowToJson(rowData, fieldNames);
        String errorType = retrieveCategory(ex);
        String line =
                StringUtils.join(
                        new String[] {
                            content,
                            errorType,
                            gson.toJson(ex.toString()),
                            DateUtil.timestampToString(new Date())
                        },
                        FIELD_DELIMITER);
        try {
            stream.write(line.getBytes(StandardCharsets.UTF_8));
            stream.write(LINE_DELIMITER.getBytes(StandardCharsets.UTF_8));
            DFSOutputStream dfsOutputStream = (DFSOutputStream) stream.getWrappedStream();
            dfsOutputStream.hsync(syncFlags);
            return errorType;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String retrieveCategory(WriteRecordException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof NullPointerException) {
            return ERR_NULL_POINTER;
        }
        for (String keyword : PRIMARY_CONFLICT_KEYWORDS) {
            if (cause.toString().toLowerCase().contains(keyword)) {
                return ERR_PRIMARY_CONFLICT;
            }
        }
        return ERR_FORMAT_TRANSFORM;
    }

    public void open() {
        try {
            FileSystem fs = FileSystemUtil.getFileSystem(config, null, distributedCache);
            Path path = new Path(location);
            stream = fs.create(path, true);
        } catch (Exception e) {
            throw new RuntimeException("Open dirty manager error", e);
        }
    }

    public void close() {
        if (stream != null) {
            try {
                stream.flush();
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
