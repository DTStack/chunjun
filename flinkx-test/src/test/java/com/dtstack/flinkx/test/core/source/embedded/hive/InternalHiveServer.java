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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;

/**
 * @author jiangbo
 * @date 2020/3/2
 */
public class InternalHiveServer extends AbstractHiveServer {

  private final HiveServer2 hiveServer2;
  private final HiveConf conf;

  public InternalHiveServer(HiveConf conf) throws Exception {
    super(conf, getHostname(conf), getPort(conf));
    hiveServer2 = new HiveServer2();
    this.conf = conf;
  }

  @Override
  public synchronized void start() throws Exception {
    hiveServer2.init(conf);
    hiveServer2.start();
    waitForStartup(this);
  }

  @Override
  public synchronized void shutdown() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }
}
