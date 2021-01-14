package com.dtstack.flinkx.metadataphoenix5.util;

import com.dtstack.flinkx.authenticate.KerberosUtil;
import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.Properties;

import static com.dtstack.flinkx.metadataphoenix5.inputformat.Metadataphoenix5InputFormat.JDBC_PHOENIX_PREFIX;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.AUTHENTICATION_TYPE;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HADOOP_SECURITY_AUTHENTICATION;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_MASTER_KERBEROS_PRINCIPAL;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_SECURITY_AUTHENTICATION;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_SECURITY_AUTHORIZATION;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_ZOOKEEPER_QUORUM;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.KEY_PRINCIPAL;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.PHOENIX_QUERYSERVER_KERBEROS_PRINCIPAL;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.ZOOKEEPER_ZNODE_PARENT;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-13 18:02
 * @Description:
 */
public class Phoenix5Help {

    public static boolean isOpenKerberos(Map<String, Object> hadoopConfig) {
        return (StringUtils.isNotEmpty(MapUtils.getString(hadoopConfig, HBASE_MASTER_KERBEROS_PRINCIPAL)) || StringUtils.isNotEmpty(MapUtils.getString(hadoopConfig, HBASE_REGIONSERVER_KERBEROS_PRINCIPAL)) ||
                StringUtils.isNotEmpty(MapUtils.getString(hadoopConfig, KEY_PRINCIPAL)));
    }

    public static String setKerberosParams(Properties p, Map<String, Object> hbaseConfigMap, String dbUrl, String znode) {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hbaseConfigMap);
        keytabFileName = KerberosUtil.loadFile(hbaseConfigMap, keytabFileName);
        String principal = KerberosUtil.getPrincipal(hbaseConfigMap, keytabFileName);
        KerberosUtil.loadKrb5Conf(hbaseConfigMap);
        p.setProperty(HBASE_SECURITY_AUTHENTICATION, AUTHENTICATION_TYPE);
        p.setProperty(HBASE_SECURITY_AUTHORIZATION, AUTHENTICATION_TYPE);
        p.setProperty(HADOOP_SECURITY_AUTHENTICATION, AUTHENTICATION_TYPE);
        //获取zookeeper地址
        String zooKeeperQuorum = dbUrl.substring(JDBC_PHOENIX_PREFIX.length(), dbUrl.length() - znode.length() - 1);
        p.setProperty(HBASE_ZOOKEEPER_QUORUM, zooKeeperQuorum);
        p.setProperty(ZOOKEEPER_ZNODE_PARENT, znode);
        p.setProperty(HBASE_MASTER_KERBEROS_PRINCIPAL, MapUtils.getString(hbaseConfigMap, HBASE_MASTER_KERBEROS_PRINCIPAL));
        p.setProperty(HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, MapUtils.getString(hbaseConfigMap, HBASE_REGIONSERVER_KERBEROS_PRINCIPAL));
        p.setProperty(PHOENIX_QUERYSERVER_KERBEROS_PRINCIPAL, principal);
        return dbUrl + ConstantValue.COLON_SYMBOL + principal + ConstantValue.COLON_SYMBOL + keytabFileName;
    }

}
