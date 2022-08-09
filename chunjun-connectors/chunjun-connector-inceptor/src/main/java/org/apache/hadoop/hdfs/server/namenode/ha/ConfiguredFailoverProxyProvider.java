//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.hdfs.server.namenode.ha;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConfiguredFailoverProxyProvider<T> extends AbstractNNFailoverProxyProvider<T> {
    private static final Log LOG = LogFactory.getLog(ConfiguredFailoverProxyProvider.class);
    private final Configuration conf;
    private final List<ConfiguredFailoverProxyProvider.AddressRpcProxyPair<T>> proxies =
            new ArrayList();
    private final UserGroupInformation ugi;
    private final Class<T> xface;
    private int currentProxyIndex = 0;

    public ConfiguredFailoverProxyProvider(Configuration conf, URI uri, Class<T> xface) {
        Preconditions.checkArgument(
                xface.isAssignableFrom(NamenodeProtocols.class),
                "Interface class %s is not a valid NameNode protocol!");
        this.xface = xface;
        this.conf = new Configuration(conf);
        int maxRetries = this.conf.getInt("dfs.client.failover.connection.retries", 0);
        this.conf.setInt("ipc.client.connect.max.retries", maxRetries);
        int maxRetriesOnSocketTimeouts =
                this.conf.getInt("dfs.client.failover.connection.retries.on.timeouts", 0);
        this.conf.setInt("ipc.client.connect.max.retries.on.timeouts", maxRetriesOnSocketTimeouts);

        try {
            this.ugi = UserGroupInformation.getCurrentUser();
            Map<String, Map<String, InetSocketAddress>> map = DFSUtil.getHaNnRpcAddresses(conf);
            Map<String, InetSocketAddress> addressesInNN = (Map) map.get(uri.getHost());
            if (addressesInNN != null && addressesInNN.size() != 0) {
                Collection<InetSocketAddress> addressesOfNns = addressesInNN.values();
                Iterator var9 = addressesOfNns.iterator();

                while (var9.hasNext()) {
                    InetSocketAddress address = (InetSocketAddress) var9.next();
                    this.proxies.add(
                            new ConfiguredFailoverProxyProvider.AddressRpcProxyPair(address));
                }

                HAUtil.cloneDelegationTokenForLogicalUri(this.ugi, uri, addressesOfNns);
            } else {
                throw new RuntimeException(
                        "Could not find any configured addresses for URI " + uri);
            }
        } catch (IOException var11) {
            throw new RuntimeException(var11);
        }
    }

    public Class<T> getInterface() {
        return this.xface;
    }

    public synchronized ProxyInfo<T> getProxy() {
        ConfiguredFailoverProxyProvider.AddressRpcProxyPair<T> current =
                (ConfiguredFailoverProxyProvider.AddressRpcProxyPair)
                        this.proxies.get(this.currentProxyIndex);
        if (current.namenode == null) {
            try {
                current.namenode =
                        NameNodeProxies.createNonHAProxy(
                                        this.conf,
                                        current.address,
                                        this.xface,
                                        this.ugi,
                                        false,
                                        this.fallbackToSimpleAuth)
                                .getProxy();
            } catch (IOException var3) {
                LOG.error("Failed to create RPC proxy to NameNode", var3);
                throw new RuntimeException(var3);
            }
        }

        return new ProxyInfo(current.namenode, current.address.toString());
    }

    public synchronized void performFailover(T currentProxy) {
        this.currentProxyIndex = (this.currentProxyIndex + 1) % this.proxies.size();
    }

    public synchronized void close() throws IOException {
        Iterator var1 = this.proxies.iterator();

        while (var1.hasNext()) {
            ConfiguredFailoverProxyProvider.AddressRpcProxyPair<T> proxy =
                    (ConfiguredFailoverProxyProvider.AddressRpcProxyPair) var1.next();
            if (proxy.namenode != null) {
                if (proxy.namenode instanceof Closeable) {
                    ((Closeable) proxy.namenode).close();
                } else {
                    RPC.stopProxy(proxy.namenode);
                }
            }
        }
    }

    public boolean useLogicalURI() {
        return true;
    }

    private static class AddressRpcProxyPair<T> {
        public final InetSocketAddress address;
        public T namenode;

        public AddressRpcProxyPair(InetSocketAddress address) {
            this.address = address;
        }
    }
}
