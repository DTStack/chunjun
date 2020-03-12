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

package com.dtstack.flinkx.test.core.source.embedded.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.shims.ShimLoader;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author jiangbo
 * @date 2020/3/2
 */
public class InternalMetaStoreServer extends AbstractHiveServer {

  private final HiveConf conf;

  private ExecutorService metaStoreExecutor = Executors.newSingleThreadExecutor();

  public InternalMetaStoreServer(HiveConf conf) throws Exception {
    super(conf, getMetastoreHostname(conf), getMetastorePort(conf));
    this.conf = conf;
  }

  @Override
  public String getURL() {
    return "jdbc:hive2://";
  }

  @Override
  public void start() throws Exception {
    startMetastore();
  }

  @Override
  public void shutdown() throws Exception {
    metaStoreExecutor.shutdown();
  }

  // async metaStore startup since Hive doesn't have that option
  private void startMetastore() throws Exception {
    Callable<Void> metastoreService = new Callable<Void>() {
      public Void call() throws Exception {
        try {
          HiveMetaStore.startMetaStore(getMetastorePort(conf),
              ShimLoader.getHadoopThriftAuthBridge(), conf);
        } catch (Throwable e) {
          throw new Exception("Error starting metastore", e);
        }
        return null;
      }
    };
    metaStoreExecutor.submit(metastoreService);
  }

  private static String getMetastoreHostname(Configuration conf)
      throws Exception {
    return new URI(conf.get(HiveConf.ConfVars.METASTOREURIS.varname)).getHost();
  }

  private static int getMetastorePort(Configuration conf) throws Exception {
    return new URI(conf.get(HiveConf.ConfVars.METASTOREURIS.varname)).getPort();
  }
}
