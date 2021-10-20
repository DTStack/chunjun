package com.dtstack.flinkx.connector.hbase14.lookup;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase14.HBaseConverter;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConf;
import com.dtstack.flinkx.connector.hbase14.util.HBaseConfigUtils;
import com.dtstack.flinkx.connector.hbase14.util.HBaseUtils;
import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.security.KerberosUtil;

import org.apache.commons.collections.MapUtils;
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
import org.hbase.async.Config;
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

    public HBaseAllTableFunction(
            HBaseConf conf,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            HBaseConverter hBaseConverter) {
        super(fieldNames, keyNames, lookupConf, hBaseConverter);
        this.hbaseConf = conf;
        hbaseConfig = conf.getHbaseConfig();
        Config config = new Config();
        config.overrideConfig(
                HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM,
                (String) conf.getHbaseConfig().get(HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM));
        config.overrideConfig(
                HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM,
                (String)
                        conf.getHbaseConfig()
                                .get(HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM));
        hbaseConfig.forEach((key, value) -> config.overrideConfig(key, (String) value));
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
                String krb5conf = KerberosUtil.getKrb5Conf(hbaseConf.getHbaseConfig());
                UserGroupInformation userGroupInformation = KerberosUtil.loginAndReturnUgi(principal, keytab, krb5conf);
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

            table = conn.getTable(TableName.valueOf(hbaseConf.getTableName()));
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
