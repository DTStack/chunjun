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
package org.apache.hadoop.hive.thrift;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.shims.Utils.TSaslServerDefinition;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSaslServerTransport.Factory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.SaslServer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Map;

public class HadoopThriftAuthBridge {
    private static final Log LOG = LogFactory.getLog(HadoopThriftAuthBridge.class);

    public HadoopThriftAuthBridge() {}

    public HadoopThriftAuthBridge.Client createClient() {
        return new HadoopThriftAuthBridge.Client();
    }

    public HadoopThriftAuthBridge.Client createClientWithConf(String authMethod) {
        UserGroupInformation ugi;
        try {
            ugi = UserGroupInformation.getLoginUser();
        } catch (IOException var4) {
            throw new IllegalStateException("Unable to get current login user: " + var4, var4);
        }

        if (this.loginUserHasCurrentAuthMethod(ugi, authMethod)) {
            LOG.debug(
                    "Not setting UGI conf as passed-in authMethod of "
                            + authMethod
                            + " = current.");
            return new HadoopThriftAuthBridge.Client();
        } else {
            LOG.debug("Setting UGI conf as passed-in authMethod of " + authMethod + " != current.");
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", authMethod);
            UserGroupInformation.setConfiguration(conf);
            return new HadoopThriftAuthBridge.Client();
        }
    }

    public HadoopThriftAuthBridge.Server createServer(String keytabFile, String principalConf)
            throws TTransportException {
        return new HadoopThriftAuthBridge.Server(keytabFile, principalConf);
    }

    public String getServerPrincipal(String principalConfig, String host) throws IOException {
        String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
        String[] names = SaslRpcServer.splitKerberosName(serverPrincipal);
        if (names.length != 3) {
            throw new IOException(
                    "Kerberos principal name does NOT have the expected hostname part: "
                            + serverPrincipal);
        } else {
            return serverPrincipal;
        }
    }

    public UserGroupInformation getCurrentUGIWithConf(String authMethod) throws IOException {
        UserGroupInformation ugi;
        try {
            ugi = UserGroupInformation.getCurrentUser();
        } catch (IOException var4) {
            throw new IllegalStateException("Unable to get current user: " + var4, var4);
        }

        if (this.loginUserHasCurrentAuthMethod(ugi, authMethod)) {
            LOG.debug(
                    "Not setting UGI conf as passed-in authMethod of "
                            + authMethod
                            + " = current.");
            return ugi;
        } else {
            LOG.debug("Setting UGI conf as passed-in authMethod of " + authMethod + " != current.");
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", authMethod);
            UserGroupInformation.setConfiguration(conf);
            return UserGroupInformation.getCurrentUser();
        }
    }

    private boolean loginUserHasCurrentAuthMethod(UserGroupInformation ugi, String sAuthMethod) {
        AuthenticationMethod authMethod;
        try {
            authMethod =
                    (AuthenticationMethod)
                            Enum.valueOf(
                                    AuthenticationMethod.class,
                                    sAuthMethod.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException var5) {
            throw new IllegalArgumentException(
                    "Invalid attribute value for hadoop.security.authentication of " + sAuthMethod,
                    var5);
        }

        LOG.debug("Current authMethod = " + ugi.getAuthenticationMethod());
        return ugi.getAuthenticationMethod().equals(authMethod);
    }

    public Map<String, String> getHadoopSaslProperties(Configuration conf) {
        return null;
    }

    public static class Server {
        public static final String DELEGATION_TOKEN_GC_INTERVAL =
                "hive.cluster.delegation.token.gc-interval";
        private static final long DELEGATION_TOKEN_GC_INTERVAL_DEFAULT = 3600000L;
        public static final String DELEGATION_KEY_UPDATE_INTERVAL_KEY =
                "hive.cluster.delegation.key.update-interval";
        public static final long DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT = 86400000L;
        public static final String DELEGATION_TOKEN_RENEW_INTERVAL_KEY =
                "hive.cluster.delegation.token.renew-interval";
        public static final long DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 86400000L;
        public static final String DELEGATION_TOKEN_MAX_LIFETIME_KEY =
                "hive.cluster.delegation.token.max-lifetime";
        public static final long DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 604800000L;
        public static final String DELEGATION_TOKEN_STORE_CLS =
                "hive.cluster.delegation.token.store.class";
        public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_STR =
                "hive.cluster.delegation.token.store.zookeeper.connectString";
        public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_STR_ALTERNATE =
                "hive.zookeeper.quorum";
        public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_TIMEOUTMILLIS =
                "hive.cluster.delegation.token.store.zookeeper.connectTimeoutMillis";
        public static final String DELEGATION_TOKEN_STORE_ZK_ZNODE =
                "hive.cluster.delegation.token.store.zookeeper.znode";
        public static final String DELEGATION_TOKEN_STORE_ZK_ACL =
                "hive.cluster.delegation.token.store.zookeeper.acl";
        public static final String DELEGATION_TOKEN_STORE_ZK_ZNODE_DEFAULT = "/hivedelegation";
        protected final UserGroupInformation realUgi;
        protected DelegationTokenSecretManager secretManager;
        static final ThreadLocal<InetAddress> remoteAddress =
                new ThreadLocal<InetAddress>() {
                    protected synchronized InetAddress initialValue() {
                        return null;
                    }
                };
        static final ThreadLocal<AuthenticationMethod> authenticationMethod =
                new ThreadLocal<AuthenticationMethod>() {
                    protected synchronized AuthenticationMethod initialValue() {
                        return AuthenticationMethod.TOKEN;
                    }
                };
        private static ThreadLocal<String> remoteUser =
                new ThreadLocal<String>() {
                    protected synchronized String initialValue() {
                        return null;
                    }
                };

        public Server() throws TTransportException {
            try {
                this.realUgi = UserGroupInformation.getCurrentUser();
            } catch (IOException var2) {
                throw new TTransportException(var2);
            }
        }

        protected Server(String keytabFile, String principalConf) throws TTransportException {
            if (keytabFile != null && !keytabFile.isEmpty()) {
                if (principalConf != null && !principalConf.isEmpty()) {
                    try {
                        String kerberosName =
                                SecurityUtil.getServerPrincipal(principalConf, "0.0.0.0");
                        UserGroupInformation.loginUserFromKeytab(kerberosName, keytabFile);
                        this.realUgi = UserGroupInformation.getLoginUser();

                        assert this.realUgi.isFromKeytab();

                    } catch (IOException var5) {
                        throw new TTransportException(var5);
                    }
                } else {
                    throw new TTransportException("No principal specified");
                }
            } else {
                throw new TTransportException("No keytab specified");
            }
        }

        public TTransportFactory createTransportFactory(
                Map<String, String> saslProps, TSaslServerDefinition... definitions)
                throws TTransportException {
            String kerberosName = this.realUgi.getUserName();
            String[] names = SaslRpcServer.splitKerberosName(kerberosName);
            if (names.length != 3) {
                throw new TTransportException(
                        "Kerberos principal should have 3 parts: " + kerberosName);
            } else {
                Factory transFactory = new Factory();
                transFactory.addServerDefinition(
                        AuthMethod.KERBEROS.getMechanismName(),
                        names[0],
                        names[1],
                        saslProps,
                        new SaslGssCallbackHandler());
                transFactory.addServerDefinition(
                        AuthMethod.DIGEST.getMechanismName(),
                        (String) null,
                        "default",
                        saslProps,
                        new HadoopThriftAuthBridge.Server.SaslDigestCallbackHandler(
                                this.secretManager));
                TSaslServerDefinition[] arr$ = definitions;
                int len$ = definitions.length;

                for (int i$ = 0; i$ < len$; ++i$) {
                    TSaslServerDefinition definition = arr$[i$];
                    transFactory.addServerDefinition(
                            definition.getMechanism(),
                            definition.getProtocol(),
                            definition.getServerName(),
                            definition.getProps(),
                            definition.getCbh());
                }

                return new HadoopThriftAuthBridge.Server.TUGIAssumingTransportFactory(
                        transFactory, this.realUgi);
            }
        }

        public TProcessor wrapProcessor(TProcessor processor) {
            return new HadoopThriftAuthBridge.Server.TUGIAssumingProcessor(
                    processor, this.secretManager, true);
        }

        public TProcessor wrapNonAssumingProcessor(TProcessor processor) {
            return new HadoopThriftAuthBridge.Server.TUGIAssumingProcessor(
                    processor, this.secretManager, false);
        }

        protected DelegationTokenStore getTokenStore(Configuration conf) throws IOException {
            String tokenStoreClassName = conf.get("hive.cluster.delegation.token.store.class", "");
            if (StringUtils.isBlank(tokenStoreClassName)) {
                return new MemoryTokenStore();
            } else {
                try {
                    Class<? extends DelegationTokenStore> storeClass =
                            Class.forName(tokenStoreClassName)
                                    .asSubclass(DelegationTokenStore.class);
                    return (DelegationTokenStore) ReflectionUtils.newInstance(storeClass, conf);
                } catch (ClassNotFoundException var4) {
                    throw new IOException(
                            "Error initializing delegation token store: " + tokenStoreClassName,
                            var4);
                }
            }
        }

        public void startDelegationTokenSecretManager(
                Configuration conf, Object rawStore, HadoopThriftAuthBridge.Server.ServerMode smode)
                throws IOException {
            long secretKeyInterval =
                    conf.getLong("hive.cluster.delegation.key.update-interval", 86400000L);
            long tokenMaxLifetime =
                    conf.getLong("hive.cluster.delegation.token.max-lifetime", 604800000L);
            long tokenRenewInterval =
                    conf.getLong("hive.cluster.delegation.token.renew-interval", 86400000L);
            long tokenGcInterval =
                    conf.getLong("hive.cluster.delegation.token.gc-interval", 3600000L);
            DelegationTokenStore dts = this.getTokenStore(conf);
            dts.init(rawStore, smode);
            this.secretManager =
                    new TokenStoreDelegationTokenSecretManager(
                            secretKeyInterval,
                            tokenMaxLifetime,
                            tokenRenewInterval,
                            tokenGcInterval,
                            dts);
            this.secretManager.startThreads();
        }

        public String getDelegationToken(String owner, final String renewer)
                throws IOException, InterruptedException {
            if (!((AuthenticationMethod) authenticationMethod.get())
                    .equals(AuthenticationMethod.KERBEROS)) {
                throw new AuthorizationException(
                        "Delegation Token can be issued only with kerberos authentication. Current AuthenticationMethod: "
                                + authenticationMethod.get());
            } else {
                UserGroupInformation currUser = UserGroupInformation.getCurrentUser();
                UserGroupInformation ownerUgi = UserGroupInformation.createRemoteUser(owner);
                if (!ownerUgi.getShortUserName().equals(currUser.getShortUserName())) {
                    ownerUgi =
                            UserGroupInformation.createProxyUser(
                                    owner, UserGroupInformation.getCurrentUser());
                    InetAddress remoteAddr = this.getRemoteAddress();
                    ProxyUsers.authorize(
                            ownerUgi, remoteAddr.getHostAddress(), (Configuration) null);
                }

                return (String)
                        ownerUgi.doAs(
                                new PrivilegedExceptionAction<String>() {
                                    public String run() throws IOException {
                                        return Server.this.secretManager.getDelegationToken(
                                                renewer);
                                    }
                                });
            }
        }

        public String getDelegationTokenWithService(String owner, String renewer, String service)
                throws IOException, InterruptedException {
            String token = this.getDelegationToken(owner, renewer);
            return Utils.addServiceToToken(token, service);
        }

        public long renewDelegationToken(String tokenStrForm) throws IOException {
            if (!((AuthenticationMethod) authenticationMethod.get())
                    .equals(AuthenticationMethod.KERBEROS)) {
                throw new AuthorizationException(
                        "Delegation Token can be issued only with kerberos authentication. Current AuthenticationMethod: "
                                + authenticationMethod.get());
            } else {
                return this.secretManager.renewDelegationToken(tokenStrForm);
            }
        }

        public String getUserFromToken(String tokenStr) throws IOException {
            return this.secretManager.getUserFromToken(tokenStr);
        }

        public void cancelDelegationToken(String tokenStrForm) throws IOException {
            this.secretManager.cancelDelegationToken(tokenStrForm);
        }

        public InetAddress getRemoteAddress() {
            return (InetAddress) remoteAddress.get();
        }

        public String getRemoteUser() {
            return (String) remoteUser.get();
        }

        static class TUGIAssumingTransportFactory extends TTransportFactory {
            private final UserGroupInformation ugi;
            private final TTransportFactory wrapped;

            public TUGIAssumingTransportFactory(
                    TTransportFactory wrapped, UserGroupInformation ugi) {
                assert wrapped != null;

                assert ugi != null;

                this.wrapped = wrapped;
                this.ugi = ugi;
            }

            public TTransport getTransport(final TTransport trans) {
                return (TTransport)
                        this.ugi.doAs(
                                new PrivilegedAction<TTransport>() {
                                    public TTransport run() {
                                        return TUGIAssumingTransportFactory.this.wrapped
                                                .getTransport(trans);
                                    }
                                });
            }
        }

        protected class TUGIAssumingProcessor implements TProcessor {
            final TProcessor wrapped;
            DelegationTokenSecretManager secretManager;
            boolean useProxy;

            TUGIAssumingProcessor(
                    TProcessor wrapped,
                    DelegationTokenSecretManager secretManager,
                    boolean useProxy) {
                this.wrapped = wrapped;
                this.secretManager = secretManager;
                this.useProxy = useProxy;
            }

            public boolean process(final TProtocol inProt, final TProtocol outProt)
                    throws TException {
                TTransport trans = inProt.getTransport();
                if (!(trans instanceof TSaslServerTransport)) {
                    throw new TException("Unexpected non-SASL transport " + trans.getClass());
                } else {
                    TSaslServerTransport saslTrans = (TSaslServerTransport) trans;
                    SaslServer saslServer = saslTrans.getSaslServer();
                    String authId = saslServer.getAuthorizationID();
                    HadoopThriftAuthBridge.Server.authenticationMethod.set(
                            AuthenticationMethod.KERBEROS);
                    HadoopThriftAuthBridge.LOG.debug("AUTH ID ======>" + authId);
                    String endUser = authId;
                    if (saslServer.getMechanismName().equals("DIGEST-MD5")) {
                        try {
                            TokenIdentifier tokenId =
                                    SaslRpcServer.getIdentifier(authId, this.secretManager);
                            endUser = tokenId.getUser().getUserName();
                            HadoopThriftAuthBridge.Server.authenticationMethod.set(
                                    AuthenticationMethod.TOKEN);
                        } catch (InvalidToken var24) {
                            throw new TException(var24.getMessage());
                        }
                    }

                    Socket socket =
                            ((TSocket) ((TSocket) saslTrans.getUnderlyingTransport())).getSocket();
                    HadoopThriftAuthBridge.Server.remoteAddress.set(socket.getInetAddress());
                    UserGroupInformation clientUgi = null;

                    boolean var11;
                    try {
                        if (this.useProxy) {
                            clientUgi =
                                    UserGroupInformation.createProxyUser(
                                            endUser, UserGroupInformation.getLoginUser());
                            HadoopThriftAuthBridge.Server.remoteUser.set(
                                    clientUgi.getShortUserName());
                            HadoopThriftAuthBridge.LOG.debug(
                                    "Set remoteUser :"
                                            + (String)
                                                    HadoopThriftAuthBridge.Server.remoteUser.get());
                            boolean var29 =
                                    (Boolean)
                                            clientUgi.doAs(
                                                    new PrivilegedExceptionAction<Boolean>() {
                                                        public Boolean run() {
                                                            try {
                                                                return TUGIAssumingProcessor.this
                                                                        .wrapped.process(
                                                                        inProt, outProt);
                                                            } catch (TException var2) {
                                                                throw new RuntimeException(var2);
                                                            }
                                                        }
                                                    });
                            return var29;
                        }

                        UserGroupInformation endUserUgi =
                                UserGroupInformation.createRemoteUser(endUser);
                        HadoopThriftAuthBridge.Server.remoteUser.set(endUserUgi.getShortUserName());
                        HadoopThriftAuthBridge.LOG.debug(
                                "Set remoteUser :"
                                        + (String) HadoopThriftAuthBridge.Server.remoteUser.get()
                                        + ", from endUser :"
                                        + endUser);
                        var11 = this.wrapped.process(inProt, outProt);
                    } catch (RuntimeException var25) {
                        if (var25.getCause() instanceof TException) {
                            throw (TException) var25.getCause();
                        }

                        throw var25;
                    } catch (InterruptedException var26) {
                        throw new RuntimeException(var26);
                    } catch (IOException var27) {
                        throw new RuntimeException(var27);
                    } finally {
                        if (clientUgi != null) {
                            try {
                                FileSystem.closeAllForUGI(clientUgi);
                            } catch (IOException var23) {
                                HadoopThriftAuthBridge.LOG.error(
                                        "Could not clean up file-system handles for UGI: "
                                                + clientUgi,
                                        var23);
                            }
                        }
                    }

                    return var11;
                }
            }
        }

        static class SaslDigestCallbackHandler implements CallbackHandler {
            private final DelegationTokenSecretManager secretManager;

            public SaslDigestCallbackHandler(DelegationTokenSecretManager secretManager) {
                this.secretManager = secretManager;
            }

            private char[] getPassword(DelegationTokenIdentifier tokenid) throws InvalidToken {
                return this.encodePassword(this.secretManager.retrievePassword(tokenid));
            }

            private char[] encodePassword(byte[] password) {
                return (new String(Base64.encodeBase64(password))).toCharArray();
            }

            public void handle(Callback[] callbacks)
                    throws InvalidToken, UnsupportedCallbackException {
                NameCallback nc = null;
                PasswordCallback pc = null;
                AuthorizeCallback ac = null;
                Callback[] arr$ = callbacks;
                int len$ = callbacks.length;

                for (int i$ = 0; i$ < len$; ++i$) {
                    Callback callback = arr$[i$];
                    if (callback instanceof AuthorizeCallback) {
                        ac = (AuthorizeCallback) callback;
                    } else if (callback instanceof NameCallback) {
                        nc = (NameCallback) callback;
                    } else if (callback instanceof PasswordCallback) {
                        pc = (PasswordCallback) callback;
                    } else if (!(callback instanceof RealmCallback)) {
                        throw new UnsupportedCallbackException(
                                callback, "Unrecognized SASL DIGEST-MD5 Callback");
                    }
                }

                if (pc != null) {
                    DelegationTokenIdentifier tokenIdentifier =
                            (DelegationTokenIdentifier)
                                    SaslRpcServer.getIdentifier(
                                            nc.getDefaultName(), this.secretManager);
                    char[] password = this.getPassword(tokenIdentifier);
                    if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
                        HadoopThriftAuthBridge.LOG.debug(
                                "SASL server DIGEST-MD5 callback: setting password for client: "
                                        + tokenIdentifier.getUser());
                    }

                    pc.setPassword(password);
                }

                if (ac != null) {
                    String authid = ac.getAuthenticationID();
                    String authzid = ac.getAuthorizationID();
                    if (authid.equals(authzid)) {
                        ac.setAuthorized(true);
                    } else {
                        ac.setAuthorized(false);
                    }

                    if (ac.isAuthorized()) {
                        if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
                            String username =
                                    ((DelegationTokenIdentifier)
                                                    SaslRpcServer.getIdentifier(
                                                            authzid, this.secretManager))
                                            .getUser()
                                            .getUserName();
                            HadoopThriftAuthBridge.LOG.debug(
                                    "SASL server DIGEST-MD5 callback: setting canonicalized client ID: "
                                            + username);
                        }

                        ac.setAuthorizedID(authzid);
                    }
                }
            }
        }

        public static enum ServerMode {
            HIVESERVER2,
            METASTORE;

            private ServerMode() {}
        }
    }

    public static class Client {
        public Client() {}

        public TTransport createClientTransport(
                String principalConfig,
                String host,
                String methodStr,
                String tokenStrForm,
                TTransport underlyingTransport,
                Map<String, String> saslProps)
                throws IOException {
            AuthMethod method = (AuthMethod) AuthMethod.valueOf(AuthMethod.class, methodStr);
            TTransport saslTransport = null;
            switch (method) {
                case DIGEST:
                    Token<DelegationTokenIdentifier> t = new Token();
                    t.decodeFromUrlString(tokenStrForm);
                    saslTransport =
                            new TSaslClientTransport(
                                    method.getMechanismName(),
                                    (String) null,
                                    (String) null,
                                    "default",
                                    saslProps,
                                    new HadoopThriftAuthBridge.Client.SaslClientCallbackHandler(t),
                                    underlyingTransport);
                    return new TUGIAssumingTransport(
                            saslTransport, UserGroupInformation.getCurrentUser());
                case KERBEROS:
                    String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
                    String[] names = SaslRpcServer.splitKerberosName(serverPrincipal);
                    if (names.length != 3) {
                        throw new IOException(
                                "Kerberos principal name does NOT have the expected hostname part: "
                                        + serverPrincipal);
                    } else {
                        Class c = NetUtils.class;
                        Method method1;
                        try {
                            method1 = c.getDeclaredMethod("canonicalizeHost", String.class);
                            method1.setAccessible(true);
                        } catch (NoSuchMethodException e) {
                            throw new IOException("NO SUCH METHOD");
                        }

                        try {
                            saslTransport =
                                    new TSaslClientTransport(
                                            method.getMechanismName(),
                                            (String) null,
                                            names[0],
                                            method1.invoke(NetUtils.class, names[1]).toString(),
                                            saslProps,
                                            (CallbackHandler) null,
                                            underlyingTransport);
                            method1.setAccessible(false);
                            return new TUGIAssumingTransport(
                                    saslTransport, UserGroupInformation.getCurrentUser());
                        } catch (Exception var13) {
                            throw new IOException(
                                    "Could not instantiate SASL transport, " + names[1], var13);
                        }
                    }
                default:
                    throw new IOException("Unsupported authentication method: " + method);
            }
        }

        private static class SaslClientCallbackHandler implements CallbackHandler {
            private final String userName;
            private final char[] userPassword;

            public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
                this.userName = encodeIdentifier(token.getIdentifier());
                this.userPassword = encodePassword(token.getPassword());
            }

            public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
                NameCallback nc = null;
                PasswordCallback pc = null;
                RealmCallback rc = null;
                Callback[] arr$ = callbacks;
                int len$ = callbacks.length;

                for (int i$ = 0; i$ < len$; ++i$) {
                    Callback callback = arr$[i$];
                    if (!(callback instanceof RealmChoiceCallback)) {
                        if (callback instanceof NameCallback) {
                            nc = (NameCallback) callback;
                        } else if (callback instanceof PasswordCallback) {
                            pc = (PasswordCallback) callback;
                        } else {
                            if (!(callback instanceof RealmCallback)) {
                                throw new UnsupportedCallbackException(
                                        callback, "Unrecognized SASL client callback");
                            }

                            rc = (RealmCallback) callback;
                        }
                    }
                }

                if (nc != null) {
                    if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
                        HadoopThriftAuthBridge.LOG.debug(
                                "SASL client callback: setting username: " + this.userName);
                    }

                    nc.setName(this.userName);
                }

                if (pc != null) {
                    if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
                        HadoopThriftAuthBridge.LOG.debug(
                                "SASL client callback: setting userPassword");
                    }

                    pc.setPassword(this.userPassword);
                }

                if (rc != null) {
                    if (HadoopThriftAuthBridge.LOG.isDebugEnabled()) {
                        HadoopThriftAuthBridge.LOG.debug(
                                "SASL client callback: setting realm: " + rc.getDefaultText());
                    }

                    rc.setText(rc.getDefaultText());
                }
            }

            static String encodeIdentifier(byte[] identifier) {
                return new String(Base64.encodeBase64(identifier));
            }

            static char[] encodePassword(byte[] password) {
                return (new String(Base64.encodeBase64(password))).toCharArray();
            }
        }
    }
}
