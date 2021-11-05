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

package com.dtstack.flinkx.connector.hbase14.table.lookup;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase.HBaseSerde;
import com.dtstack.flinkx.connector.hbase.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConf;
import com.dtstack.flinkx.connector.hbase14.util.HBaseConfigUtils;
import com.dtstack.flinkx.connector.hbase14.util.HBaseUtils;
import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.security.KerberosUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.dtstack.flinkx.connector.hbase14.util.HBaseConfigUtils.KEY_PRINCIPAL;

public class HBaseAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseAllTableFunction.class);
    private final HBaseConf hbaseConf;
    private Connection conn;
    private Table table;
    private ResultScanner resultScanner;
    private final transient Map<String, Object> hbaseConfig;

    private final HBaseTableSchema hbaseTableSchema;
    private transient HBaseSerde serde;

    public HBaseAllTableFunction(
            HBaseConf conf,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            HBaseTableSchema hbaseTableSchema) {
        super(fieldNames, keyNames, lookupConf, null);
        this.hbaseConf = conf;
        this.hbaseTableSchema = hbaseTableSchema;
        hbaseConfig = conf.getHbaseConfig();
    }

    @Override
    protected void loadData(Object cacheRef) {
        Configuration conf;
        int loadDataCount = 0;
        try {
            if (HBaseConfigUtils.isEnableKerberos(hbaseConfig)) {
                conf = HBaseConfigUtils.getHadoopConfiguration(hbaseConf.getHbaseConfig());
                conf.set(
                        HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM,
                        (String)
                                hbaseConf
                                        .getHbaseConfig()
                                        .get(HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM));
                conf.set(
                        HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM,
                        (String)
                                hbaseConf
                                        .getHbaseConfig()
                                        .get(HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM));

                String principal = HBaseConfigUtils.getPrincipal(hbaseConf.getHbaseConfig());

                HBaseConfigUtils.fillSyncKerberosConfig(conf, hbaseConf.getHbaseConfig());
                String keytab =
                        HBaseConfigUtils.loadKeyFromConf(
                                hbaseConf.getHbaseConfig(), HBaseConfigUtils.KEY_KEY_TAB);

                LOG.info("kerberos principal:{}，keytab:{}", principal, keytab);

                conf.set(HBaseConfigUtils.KEY_HBASE_CLIENT_KEYTAB_FILE, keytab);
                conf.set(HBaseConfigUtils.KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL, principal);

                UserGroupInformation userGroupInformation =
                        KerberosUtil.loginAndReturnUgi(conf.get(KEY_PRINCIPAL), principal, keytab);
                Configuration finalConf = conf;
                conn =
                        userGroupInformation.doAs(
                                (PrivilegedAction<Connection>)
                                        () -> {
                                            try {
                                                ScheduledChore authChore =
                                                        AuthUtil.getAuthChore(finalConf);
                                                if (authChore != null) {
                                                    ChoreService choreService =
                                                            new ChoreService("hbaseKerberosSink");
                                                    choreService.scheduleChore(authChore);
                                                }

                                                return ConnectionFactory.createConnection(
                                                        finalConf);

                                            } catch (IOException e) {
                                                LOG.error(
                                                        "Get connection fail with config:{}",
                                                        finalConf);
                                                throw new RuntimeException(e);
                                            }
                                        });

            } else {
                conf = HBaseConfigUtils.getConfig(hbaseConf.getHbaseConfig());
                conf.set(
                        HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM,
                        (String)
                                hbaseConf
                                        .getHbaseConfig()
                                        .get(HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM));
                conf.set(
                        HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM,
                        (String)
                                hbaseConf
                                        .getHbaseConfig()
                                        .get(HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM));
                conn = ConnectionFactory.createConnection(conf);
            }

            table = conn.getTable(TableName.valueOf(hbaseConf.getTable()));
            resultScanner = table.getScanner(new Scan());
            for (Result r : resultScanner) {
                Map<String, Object> kv = new HashMap<>();
                for (Cell cell : r.listCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    StringBuilder key = new StringBuilder();
                    key.append(family).append(":").append(qualifier);
                    Optional<String> typeOption =
                            hbaseConf.getColumnMetaInfos().stream()
                                    .filter(fieldConf -> key.toString().equals(fieldConf.getName()))
                                    .map(FieldConf::getType)
                                    .findAny();
                    Object value =
                            HBaseUtils.convertByte(
                                    CellUtil.cloneValue(cell),
                                    typeOption.orElseThrow(IllegalArgumentException::new));
                    kv.put((key.toString()), value);
                }
                loadDataCount++;
                fillData(kv);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            LOG.info("load Data count: {}", loadDataCount);
            try {
                if (null != conn) {
                    conn.close();
                }

                if (null != table) {
                    table.close();
                }

                if (null != resultScanner) {
                    resultScanner.close();
                }
            } catch (IOException e) {
                LOG.error("", e);
            }
        }
    }
}
