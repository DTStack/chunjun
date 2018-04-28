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

package com.dtstack.flinkx.odps.writer;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.dtstack.flinkx.odps.OdpsConfigKeys;
import com.dtstack.flinkx.util.RetryUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import static com.dtstack.flinkx.odps.OdpsConfigConstants.*;

/**
 * Odps Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OdpsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtil.class);

    public static int MAX_RETRY_TIME = 10;

    public static Odps initOdps(Map<String,String> odpsConfig) {
        String odpsServer = odpsConfig.get(OdpsConfigKeys.KEY_ODPS_SERVER);
        if(StringUtils.isBlank(odpsServer)) {
            odpsServer = DEFAULT_ODPS_SERVER;
        }

        String accessId = odpsConfig.get(OdpsConfigKeys.KEY_ACCESS_ID);
        if(StringUtils.isBlank(accessId)) {
            throw new IllegalArgumentException("accessId is required");
        }

        String accessKey = odpsConfig.get(OdpsConfigKeys.KEY_ACCESS_KEY);
        if(StringUtils.isBlank(accessKey)) {
            throw new IllegalArgumentException("accessKey is required");
        }

        String project = odpsConfig.get(OdpsConfigKeys.KEY_PROJECT);
        if(StringUtils.isBlank(project)) {
            throw new IllegalArgumentException("project is required");
        }

        String packageAuthorizedProject = odpsConfig.get(PACKAGE_AUTHORIZED_PROJECT);

        String defaultProject;
        if(StringUtils.isBlank(packageAuthorizedProject)) {
            defaultProject = project;
        } else {
            defaultProject = packageAuthorizedProject;
        }

        String accountType = odpsConfig.get(OdpsConfigKeys.KEY_ACCOUNT_TYPE);
        if(StringUtils.isBlank(accountType)) {
            accountType = DEFAULT_ACCOUNT_TYPE;
        }

        Account account = null;
        if (accountType.equalsIgnoreCase(DEFAULT_ACCOUNT_TYPE)) {
            account = new AliyunAccount(accessId, accessKey);
        } else {
            throw new IllegalArgumentException("Unsupported account type: " + accountType + " (Only 'aliyun' or 'taobao' are supported");
        }

        Odps odps = new Odps(account);
        odps.getRestClient().setConnectTimeout(3);
        odps.getRestClient().setReadTimeout(3);
        odps.getRestClient().setRetryTimes(2);
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(odpsServer);

        return odps;
    }




    public static boolean isPartitionedTable(Table table) {
        //必须要是非分区表才能 truncate 整个表
        List<Column> partitionKeys;
        try {
            partitionKeys = table.getSchema().getPartitionColumns();
            if (null != partitionKeys && !partitionKeys.isEmpty()) {
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public static List<String> listOdpsPartitions(Table table) {
        List<String> parts = new ArrayList<String>();
        try {
            List<Partition> partitions = table.getPartitions();
            for(Partition partition : partitions) {
                parts.add(partition.getPartitionSpec().toString());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve all the parititons of odps destination table: " + table.getName());
        }
        return parts;
    }

    public static TableTunnel.UploadSession createMasterTunnelUpload(final TableTunnel tunnel, final String projectName,
                                                                     final String tableName, final String partition) {
        if(StringUtils.isBlank(partition)) {
            try {
                return RetryUtil.executeWithRetry(new Callable<TableTunnel.UploadSession>() {
                    @Override
                    public TableTunnel.UploadSession call() throws Exception {
                        return tunnel.createUploadSession(projectName, tableName);
                    }
                }, MAX_RETRY_TIME, 1000L, true);
            } catch (Exception e) {
                throw new RuntimeException("Failed to construct TunnelUpload", e);
            }
        } else {
            final PartitionSpec partitionSpec = new PartitionSpec(partition);
            try {
                return RetryUtil.executeWithRetry(new Callable<TableTunnel.UploadSession>() {
                    @Override
                    public TableTunnel.UploadSession call() throws Exception {
                        return tunnel.createUploadSession(projectName, tableName, partitionSpec);
                    }
                }, MAX_RETRY_TIME, 1000L, true);
            } catch (Exception e) {
                throw new RuntimeException("Failed to construct TunnelUpload", e);
            }
        }
    }

    private static boolean isPartitionExist(Table table, String partition) {
        // check if exist partition 返回值不为 null
        List<String> odpsParts = OdpsUtil.listOdpsPartitions(table);

        int j = 0;
        for (; j < odpsParts.size(); j++) {
            if (odpsParts.get(j).replaceAll("'", "").equals(partition)) {
                break;
            }
        }

        return j != odpsParts.size();
    }

    private static String getPartSpec(String partition) {
        StringBuilder partSpec = new StringBuilder();
        String[] parts = partition.split(",");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            String[] kv = part.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException("partition format should be like: pt=1,ds=hangzhou");
            }
            partSpec.append(kv[0]).append("=");
            partSpec.append("'").append(kv[1].replace("'", "")).append("'");
            if (i != parts.length - 1) {
                partSpec.append(",");
            }
        }
        return partSpec.toString();
    }

    public static void runSqlTask(Odps odps, String query) {
        if (StringUtils.isBlank(query)) {
            return;
        }

        String taskName = "flinkx_odpswriter_trunacte_" + UUID.randomUUID().toString().replace('-', '_');

        LOG.info("Try to start sqlTask:[{}] to run odps sql:[\n{}\n] .", taskName, query);

        Instance instance;
        Instance.TaskStatus status;
        try {
            instance = SQLTask.run(odps, odps.getDefaultProject(), query, taskName, null, null);
            instance.waitForSuccess();
            status = instance.getTaskStatus().get(taskName);
            if (!Instance.TaskStatus.Status.SUCCESS.equals(status.getStatus())) {
                throw new RuntimeException("Failed to run odps sql: " + query);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void dropPart(Odps odps, Table table, String partition) {
        String partSpec = getPartSpec(partition);
        StringBuilder dropPart = new StringBuilder();
        dropPart.append("alter table ").append(table.getName())
                .append(" drop IF EXISTS partition(").append(partSpec)
                .append(");");
        try {
            runSqlTask(odps, dropPart.toString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop partition " + partition +  " of table " + table.getName());
        }
    }

    public static void addPart(Odps odps, Table table, String partition) {
        String partSpec = getPartSpec(partition);
        // add if not exists partition
        StringBuilder addPart = new StringBuilder();
        addPart.append("alter table ").append(table.getName()).append(" add IF NOT EXISTS partition(")
                .append(partSpec).append(");");
        try {
            runSqlTask(odps, addPart.toString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to add partitions to odps table: " + table.getName(), e);
        }
    }


    public static void truncatePartition(Odps odps, Table table, String partition) {
        if (isPartitionExist(table, partition)) {
            dropPart(odps, table, partition);
        }
        addPart(odps, table, partition);
    }

    public static void truncateNonPartitionedTable(Odps odps, Table tab) {
        String truncateNonPartitionedTableSql = "truncate table " + tab.getName() + ";";

        try {
            runSqlTask(odps, truncateNonPartitionedTableSql);
        } catch (Exception e) {
            throw new RuntimeException("Failed to truncate odps table: " + tab.getName(), e);
        }
    }

    public static void dealTruncate(Odps odps, Table table, String partition, boolean truncate) {
        boolean isPartitionedTable = OdpsUtil.isPartitionedTable(table);

        if (truncate) {
            //需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw new IllegalArgumentException("Your destination table is a partitioned table. Please provide parition such as pt=${bizdate}");
                } else {
                    LOG.info("Try to truncate partition=[{}] in table=[{}].", partition, table.getName());
                    OdpsUtil.truncatePartition(odps, table, partition);
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw new IllegalArgumentException("Your destination table is not a partitioned table. Please remove configurations about partitions");
                } else {
                    LOG.info("Try to truncate table:[{}].", table.getName());
                    OdpsUtil.truncateNonPartitionedTable(odps, table);
                }
            }
        } else {
            //不需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw new IllegalArgumentException("Your destination table is a partitioned table. Please provide parition such as pt=${bizdate}.");
                } else {
                    boolean isPartitionExists = OdpsUtil.isPartitionExist(table, partition);
                    if (!isPartitionExists) {
                        LOG.info("Try to add partition:[{}] in table:[{}].", partition,
                                table.getName());
                        OdpsUtil.addPart(odps, table, partition);
                    }
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw new IllegalArgumentException("Your destination table is not a partitioned table. Please remove configurations about partitions");
                }
            }
        }
    }

}
