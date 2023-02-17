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

package com.dtstack.chunjun.connector.solr.client;

import org.apache.flink.runtime.security.DynamicConfiguration;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrPortAwareCookieSpecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;

import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Locale;

public class ChunJunKrb5HttpClientBuilder extends Krb5HttpClientBuilder {

    public static final String SOLR_KERBEROS_JAAS_APPNAME = "solr.kerberos.jaas.appname";
    private static final Logger logger =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final String keytab;
    private final String principal;
    // Set a buffered entity based request interceptor
    private final HttpRequestInterceptor bufferedEntityInterceptor =
            (request, context) -> {
                if (request instanceof HttpEntityEnclosingRequest) {
                    HttpEntityEnclosingRequest enclosingRequest =
                            ((HttpEntityEnclosingRequest) request);
                    HttpEntity requestEntity = enclosingRequest.getEntity();
                    enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
                }
            };

    public ChunJunKrb5HttpClientBuilder(String principal, String keytab) {
        this.principal = principal;
        this.keytab = keytab;
    }

    @Override
    public SolrHttpClientBuilder getBuilder(SolrHttpClientBuilder builder) {
        if (System.getProperty(SOLR_KERBEROS_JAAS_APPNAME) != null) {
            String configValue = System.getProperty(SOLR_KERBEROS_JAAS_APPNAME);
            //        if (System.getProperty(LOGIN_CONFIG_PROP) != null) {
            //            String configValue = System.getProperty(LOGIN_CONFIG_PROP);

            if (configValue != null) {
                logger.info("Setting up SPNego auth with config: " + configValue);
                final String useSubjectCredsProp = "javax.security.auth.useSubjectCredsOnly";
                String useSubjectCredsVal = System.getProperty(useSubjectCredsProp);

                // "javax.security.auth.useSubjectCredsOnly" should be false so that the underlying
                // authentication mechanism can load the credentials from the JAAS configuration.
                if (useSubjectCredsVal == null) {
                    System.setProperty(useSubjectCredsProp, "false");
                } else if (!useSubjectCredsVal.toLowerCase(Locale.ROOT).equals("false")) {
                    // Don't overwrite the prop value if it's already been written to something
                    // else,
                    // but log because it is likely the Credentials won't be loaded correctly.
                    logger.warn(
                            "System Property: "
                                    + useSubjectCredsProp
                                    + " set to: "
                                    + useSubjectCredsVal
                                    + " not false.  SPNego authentication may not be successful.");
                }
                appendJaasConf(configValue, keytab, principal);

                // javax.security.auth.login.Configuration.setConfiguration(jaasConfig);
                // Enable only SPNEGO authentication scheme.

                builder.setAuthSchemeRegistryProvider(
                        () -> {
                            Lookup<AuthSchemeProvider> authProviders =
                                    RegistryBuilder.<AuthSchemeProvider>create()
                                            .register(
                                                    AuthSchemes.SPNEGO,
                                                    new SPNegoSchemeFactory(true, false))
                                            .build();
                            return authProviders;
                        });
                // Get the credentials from the JAAS configuration rather than here
                Credentials useJaasCreds =
                        new Credentials() {
                            public String getPassword() {
                                return null;
                            }

                            public Principal getUserPrincipal() {
                                return null;
                            }
                        };

                HttpClientUtil.setCookiePolicy(SolrPortAwareCookieSpecFactory.POLICY_NAME);

                builder.setCookieSpecRegistryProvider(
                        () -> {
                            SolrPortAwareCookieSpecFactory cookieFactory =
                                    new SolrPortAwareCookieSpecFactory();

                            return RegistryBuilder.<CookieSpecProvider>create()
                                    .register(
                                            SolrPortAwareCookieSpecFactory.POLICY_NAME,
                                            cookieFactory)
                                    .build();
                        });

                builder.setDefaultCredentialsProvider(
                        () -> {
                            CredentialsProvider credentialsProvider =
                                    new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, useJaasCreds);
                            return credentialsProvider;
                        });
                HttpClientUtil.addRequestInterceptor(bufferedEntityInterceptor);
            }
        } else {
            logger.warn(
                    "{} is configured without specifying system property '{}'",
                    getClass().getName(),
                    LOGIN_CONFIG_PROP);
        }

        return builder;
    }

    public static synchronized void appendJaasConf(String name, String keytab, String principal) {
        javax.security.auth.login.Configuration priorConfig =
                javax.security.auth.login.Configuration.getConfiguration();
        // construct a dynamic JAAS configuration
        DynamicConfiguration currentConfig = new DynamicConfiguration(priorConfig);
        // wire up the configured JAAS login contexts to use the krb5 entries
        AppConfigurationEntry krb5Entry =
                org.apache.flink.runtime.security.KerberosUtils.keytabEntry(keytab, principal);
        currentConfig.addAppConfigurationEntry(name, krb5Entry);
        javax.security.auth.login.Configuration.setConfiguration(currentConfig);
    }
}
