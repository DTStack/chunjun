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


package com.dtstack.flinkx.test.core.source.embedded;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.test.core.source.embedded.hive.InternalHiveServer;
import com.dtstack.flinkx.test.core.source.embedded.hive.InternalMetaStoreServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 启动Hive服务
 * TODO 未完成 引入的hive包版本不统一，hive server无法启动
 *
 * @author jiangbo
 * @date 2020/2/29
 */
public class EmbeddedHiveService {

    public static Logger LOG = LoggerFactory.getLogger(EmbeddedHiveService.class);

    public EmbeddedHiveService() {
    }

    public static void main(String[] args) {
        LoggerContext loggerContext= (LoggerContext) LoggerFactory.getILoggerFactory();
        //设置全局日志级别
        ch.qos.logback.classic.Logger logger=loggerContext.getLogger("root");
        logger.setLevel(Level.toLevel("error"));

        EmbeddedHiveService hiveService = new EmbeddedHiveService();
        hiveService.startService();
    }

    public JSONObject startService() {
        JSONObject hadoopConfig = new JSONObject();

        try {
            EmbeddedHDFSService hdfsService = new EmbeddedHDFSService();
            hadoopConfig = hdfsService.startService();
        } catch (Exception e) {
            LOG.warn("启动HDFS服务失败", e);
            return hadoopConfig;
        }

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db;create=true");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        hiveConf.set("hive.metastore.local", "true");
        hiveConf.set("hive.metastore.warehouse.dir", "/user/hive/warehouse");

        hadoopConfig.forEach((key, val) -> {
            if (null != val) {
                hiveConf.set(key, val.toString());
            }
        });

        startMetaStore(hiveConf);
        startHiveServer2(hiveConf);

        return hadoopConfig;
    }

    private void startMetaStore(HiveConf hiveConf){
        hiveConf.set("hive.metastore.uris", "thrift://127.0.0.1:9083");

        InternalMetaStoreServer metaStore = null;
        try {
            metaStore = new InternalMetaStoreServer(hiveConf);
            metaStore.start();
        } catch (Exception e) {
            LOG.error("", e);
            if (metaStore != null) {
                try {
                    metaStore.shutdown();
                } catch (Exception ex) {
                    LOG.error("", ex);
                }
            }
        }
    }

    private void startHiveServer2(HiveConf hiveConf){
        InternalHiveServer server = null;
        try{
            server = new InternalHiveServer(hiveConf);
            server.start();
        } catch (Exception e){
            LOG.error("", e);
            if (server != null){
                try {
                    server.shutdown();
                } catch (Exception ex) {
                    LOG.error("", ex);
                }
            }
        }
    }
}
