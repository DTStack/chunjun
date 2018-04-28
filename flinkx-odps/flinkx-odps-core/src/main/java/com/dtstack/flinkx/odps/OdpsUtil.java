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

package com.dtstack.flinkx.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Partition;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.dtstack.flinkx.util.RangeSplitUtil;
import com.dtstack.flinkx.util.RetryUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import static com.dtstack.flinkx.odps.OdpsConfigConstants.DEFAULT_ACCOUNT_TYPE;
import static com.dtstack.flinkx.odps.OdpsConfigConstants.DEFAULT_ODPS_SERVER;
import static com.dtstack.flinkx.odps.OdpsConfigConstants.PACKAGE_AUTHORIZED_PROJECT;

/**
 * Odps Utilities used for OdpsWriter and OdpsReader
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
            throw new IllegalArgumentException("Unsupported account type: " + accountType);
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
        return getPartitionDepth(table) > 0;
    }

    public static int getPartitionDepth(Table table) {
        TableSchema tableSchema = table.getSchema();
        return tableSchema.getPartitionColumns().size();
    }

    public static List<String> getTableAllPartitions(Table table) {
        List<Partition> tableAllPartitions = table.getPartitions();

        List<String> retPartitions = new ArrayList<String>();

        if (null != tableAllPartitions) {
            for (Partition partition : tableAllPartitions) {
                retPartitions.add(partition.getPartitionSpec().toString());
            }
        }

        return retPartitions;
    }

    public static Table getTable(Odps odps, String projectName, String tableName) {
        Table table = odps.tables().get(projectName, tableName);
        try {
            table.getOwner();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    public static TableTunnel.DownloadSession createMasterSessionForNonPartitionedTable(Odps odps, String tunnelServer,
                                                                                        final String projectName, final String tableName) {
        final TableTunnel tunnel = new TableTunnel(odps);

        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        try {
            return RetryUtil.executeWithRetry(new Callable<TableTunnel.DownloadSession>() {
                @Override
                public TableTunnel.DownloadSession call() throws Exception {
                    return tunnel.createDownloadSession(
                            projectName, tableName);
                }
            }, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static TableTunnel.DownloadSession createMasterSessionForPartitionedTable(Odps odps, String tunnelServer,
                                                                                     final String projectName, final String tableName, String partition) {

        final TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        final PartitionSpec partitionSpec = new PartitionSpec(partition);

        try {
            return RetryUtil.executeWithRetry(new Callable<TableTunnel.DownloadSession>() {
                @Override
                public TableTunnel.DownloadSession call() throws Exception {
                    return tunnel.createDownloadSession(
                            projectName, tableName, partitionSpec);
                }
            }, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }



    public static TableTunnel.DownloadSession getSlaveSessionForNonPartitionedTable(Odps odps, final String sessionId,
                                                                                    String tunnelServer, final String projectName, final String tableName) {
        final TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        try {
            return RetryUtil.executeWithRetry(new Callable<TableTunnel.DownloadSession>() {
                @Override
                public TableTunnel.DownloadSession call() throws Exception {
                    return tunnel.getDownloadSession(
                            projectName, tableName, sessionId);
                }
            }, MAX_RETRY_TIME ,1000, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TableTunnel.DownloadSession getSlaveSessionForPartitionedTable(Odps odps, final String sessionId,
                                                                                 String tunnelServer, final String projectName, final String tableName, String partition) {
        final TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        final PartitionSpec partitionSpec = new PartitionSpec(partition);
        try {
            return RetryUtil.executeWithRetry(new Callable<TableTunnel.DownloadSession>() {
                @Override
                public TableTunnel.DownloadSession call() throws Exception {
                    return tunnel.getDownloadSession(
                            projectName, tableName, partitionSpec, sessionId);
                }
            }, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    public static RecordReader getRecordReader(final TableTunnel.DownloadSession downloadSession, final long start, final long count,
                                               final boolean isCompress) {
        try {
            return downloadSession.openRecordReader(start, count, isCompress);
        } catch (Exception e) {
            throw new RuntimeException("Fail to open RecordRecord.", e);
        }

    }

    /**
     * Pair left: startIndex, right: stepCount
     */
    public static List<Pair<Long, Long>> splitRecordCount(long recordCount, int adviceNum) {
        if(recordCount<0){
            throw new IllegalArgumentException("切分的 recordCount 不能为负数.recordCount=" + recordCount);
        }

        if(adviceNum<1){
            throw new IllegalArgumentException("切分的 adviceNum 不能为负数.adviceNum=" + adviceNum);
        }

        List<Pair<Long, Long>> result = new ArrayList<Pair<Long, Long>>();
        // 为了适配 RangeSplitUtil 的处理逻辑，起始值从0开始计算
        if (recordCount == 0) {
            result.add(ImmutablePair.of(0L, 0L));
            return result;
        }

        long[] tempResult = RangeSplitUtil.doLongSplit(0L, recordCount - 1, adviceNum);

        tempResult[tempResult.length - 1]++;

        for (int i = 0; i < tempResult.length - 1; i++) {
            result.add(ImmutablePair.of(tempResult[i], (tempResult[i + 1] - tempResult[i])));
        }
        return result;
    }

}
