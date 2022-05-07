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

package com.dtstack.flinkx.connector.solr.client;

import com.dtstack.flinkx.connector.solr.SolrConf;
import com.dtstack.flinkx.security.KerberosConfig;
import com.dtstack.flinkx.security.KerberosUtil;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.runtime.security.DynamicConfiguration;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Optional;

import static com.dtstack.flinkx.connector.solr.client.FlinkxKrb5HttpClientBuilder.SOLR_KERBEROS_JAAS_APPNAME;

/**
 * A CloudSolrClient wrapper for kerberos auth.
 *
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/15
 */
public class CloudSolrClientKerberosWrapper extends SolrClient {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String JAAS_APP_NAME = "SolrJClient";
    private final SolrConf solrConf;
    private CloudSolrClient cloudSolrClient;
    private Subject subject;
    private DistributedCache distributedCache;

    public CloudSolrClientKerberosWrapper(SolrConf solrConf, DistributedCache distributedCache) {
        this.solrConf = solrConf;
        this.distributedCache = distributedCache;
    }

    public void init() {
        if (solrConf.getKerberosConfig() != null) {
            try {
                initKerberos();
            } catch (LoginException e) {
                throw new FlinkxRuntimeException(e);
            }
            doWithKerberos(
                    () -> {
                        connect();
                        return null;
                    });
        } else {
            connect();
        }
    }

    private <T> T doWithKerberos(PrivilegedAction<T> func) {
        return Subject.doAs(subject, func);
    }

    private void doWithKerberosException(PrivilegedExceptionAction<Object> func)
            throws PrivilegedActionException {
        Subject.doAs(subject, func);
    }

    @Override
    public UpdateResponse add(SolrInputDocument solrDocument) {
        return doWithKerberos(
                () -> {
                    try {
                        return cloudSolrClient.add(solrDocument);
                    } catch (SolrServerException | IOException e) {
                        throw new FlinkxRuntimeException(e);
                    }
                });
    }

    @Override
    public UpdateResponse commit() {
        return doWithKerberos(
                () -> {
                    try {
                        return cloudSolrClient.commit();
                    } catch (SolrServerException | IOException e) {
                        throw new FlinkxRuntimeException(e);
                    }
                });
    }

    @Override
    public QueryResponse query(SolrParams params) {
        return doWithKerberos(
                () -> {
                    try {
                        return cloudSolrClient.query(params);
                    } catch (SolrServerException | IOException e) {
                        throw new FlinkxRuntimeException(e);
                    }
                });
    }

    @Override
    public NamedList<Object> request(SolrRequest request, String collection)
            throws SolrServerException, IOException {
        throw new FlinkxRuntimeException("do not support");
    }

    @Override
    public void close() {
        doWithKerberos(
                () -> {
                    try {
                        cloudSolrClient.close();
                    } catch (IOException e) {
                        throw new FlinkxRuntimeException(e);
                    }
                    return null;
                });
    }

    private void connect() {
        cloudSolrClient =
                new CloudSolrClient.Builder(
                                solrConf.getZkHosts(), Optional.ofNullable(solrConf.getZkChroot()))
                        .build();
        String collectionName = solrConf.getCollection();
        cloudSolrClient.setDefaultCollection(collectionName);
        cloudSolrClient.connect();
    }

    private void initKerberos() throws LoginException {
        KerberosConfig kerberosConfig = solrConf.getKerberosConfig();
        Map<String, Object> kerberosConfigMap =
                GsonUtil.GSON.fromJson(GsonUtil.GSON.toJson(kerberosConfig), Map.class);
        String principal = kerberosConfig.getPrincipal();
        String krb5conf = loadKrbFile(kerberosConfigMap, kerberosConfig.getKrb5conf());
        String keytab = loadKrbFile(kerberosConfigMap, kerberosConfig.getKeytab());

        System.setProperty(SOLR_KERBEROS_JAAS_APPNAME, JAAS_APP_NAME);
        KerberosUtil.reloadKrb5conf(krb5conf);
        subject = createSubject(principal, keytab);
        setKrb5HttpClient(principal, keytab);
        LOG.info("Kerberos login principal: {}, keytab: {}", principal, keytab);
    }

    public String loadKrbFile(Map<String, Object> kerberosConfigMap, String filePath) {
        try {
            KerberosUtil.checkFileExists(filePath);
            return filePath;
        } catch (Exception e) {
            return KerberosUtil.loadFile(kerberosConfigMap, filePath, distributedCache);
        }
    }

    private void setKrb5HttpClient(String principal, String keytab) {
        Krb5HttpClientBuilder krbBuilder = new FlinkxKrb5HttpClientBuilder(principal, keytab);
        SolrHttpClientBuilder kb = krbBuilder.getBuilder();
        HttpClientUtil.setHttpClientBuilder(kb);
    }

    private Subject createSubject(String principal, String keytab) throws LoginException {
        // construct a dynamic JAAS configuration
        DynamicConfiguration currentConfig = new DynamicConfiguration(null);
        // wire up the configured JAAS login contexts to use the krb5 entries
        AppConfigurationEntry krb5Entry =
                org.apache.flink.runtime.security.KerberosUtils.keytabEntry(keytab, principal);
        currentConfig.addAppConfigurationEntry(JAAS_APP_NAME, krb5Entry);
        LoginContext context = new LoginContext(JAAS_APP_NAME, null, null, currentConfig);

        context.login();
        return context.getSubject();
    }
}
