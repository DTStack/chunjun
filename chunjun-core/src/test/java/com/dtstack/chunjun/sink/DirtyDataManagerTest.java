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

package com.dtstack.chunjun.sink;

import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.RowUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;

import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirtyDataManagerTest {

    @TempDir static File tempDir;

    private static final String JOB_ID = "0xDTSTACK";

    private static final GenericRowData ROW_DATA = new GenericRowData(2);
    private static final String[] FIELD_NAMES = new String[] {"id", "name"};

    private static String hdfsURI;

    static {
        ROW_DATA.setField(0, new BigDecimalColumn(1));
        ROW_DATA.setField(1, new StringColumn("test"));
    }

    private static MiniDFSCluster miniDFSCluster;

    @BeforeAll
    static void setup() throws IOException {
        Configuration hdConf = new Configuration();
        // 启动 HDFS Mini 集群。
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
        hdConf.set("dfs.block.size", String.valueOf(1048576)); // this is the minimum we can set.
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
        miniDFSCluster = builder.build();
        hdfsURI =
                "hdfs://"
                        + miniDFSCluster.getURI().getHost()
                        + ":"
                        + miniDFSCluster.getNameNodePort()
                        + "/";
    }

    @AfterAll
    static void close() throws IOException {
        miniDFSCluster.shutdown();
    }

    @Test
    public void testOpen() {
        String path = tempDir.getPath();
        Map<String, Object> configMap = new HashMap<>();
        Map<String, Future<Path>> cacheCopyTasks =
                ImmutableMap.<String, Future<Path>>builder()
                        .put("test", Futures.immediateFuture(new Path("file:///opt")))
                        .build();
        DistributedCache distributedCache = new DistributedCache(cacheCopyTasks);
        DirtyDataManager dirtyDataManager =
                new DirtyDataManager(path, configMap, FIELD_NAMES, JOB_ID, distributedCache);
        dirtyDataManager.open();
    }

    @Test
    public void testWriteData() throws IOException {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FileSystem.FS_DEFAULT_NAME_KEY, hdfsURI);
        Map<String, Future<Path>> cacheCopyTasks =
                ImmutableMap.<String, Future<Path>>builder()
                        .put("test", Futures.immediateFuture(new Path("file:///opt")))
                        .build();
        DistributedCache distributedCache = new DistributedCache(cacheCopyTasks);
        DirtyDataManager dirtyDataManager =
                new DirtyDataManager(hdfsURI, configMap, FIELD_NAMES, JOB_ID, distributedCache);
        try (DistributedFileSystem fs = miniDFSCluster.getFileSystem()) {
            dirtyDataManager.open();
            dirtyDataManager.writeData(
                    ROW_DATA, new WriteRecordException("write error", new Throwable()));
            FSDataInputStream fsDataInputStream =
                    fs.open(new org.apache.hadoop.fs.Path(dirtyDataManager.location));

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copyBytes(fsDataInputStream, out, 1024);
            String dataJson = RowUtil.rowToJson(ROW_DATA, FIELD_NAMES);
            assertTrue(out.toString().contains(dataJson));
        } finally {
            dirtyDataManager.close();
        }
    }
}
