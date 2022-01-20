package org.apache.hadoop.net;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ReflectionUtils;

import javax.net.SocketFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@LimitedPrivate({"HDFS", "MapReduce"})
@Unstable
public class NetUtils {
    private static final Log LOG = LogFactory.getLog(NetUtils.class);

    private static Map<String, String> hostToResolved = new HashMap<>();

    private static final String FOR_MORE_DETAILS_SEE = " For more details see:  ";

    public static final String UNKNOWN_HOST = "(unknown)";

    public static final String HADOOP_WIKI = "http://wiki.apache.org/hadoop/";

    public static SocketFactory getSocketFactory(Configuration conf, Class<?> clazz) {
        SocketFactory factory = null;
        String propValue = conf.get("hadoop.rpc.socket.factory.class." + clazz.getSimpleName());
        if (propValue != null && propValue.length() > 0)
            factory = getSocketFactoryFromProperty(conf, propValue);
        if (factory == null) factory = getDefaultSocketFactory(conf);
        return factory;
    }

    public static SocketFactory getDefaultSocketFactory(Configuration conf) {
        String propValue =
                conf.get(
                        "hadoop.rpc.socket.factory.class.default",
                        "org.apache.hadoop.net.StandardSocketFactory");
        if (propValue == null || propValue.length() == 0) return SocketFactory.getDefault();
        return getSocketFactoryFromProperty(conf, propValue);
    }

    public static SocketFactory getSocketFactoryFromProperty(Configuration conf, String propValue) {
        try {
            Class<?> theClass = conf.getClassByName(propValue);
            return (SocketFactory) ReflectionUtils.newInstance(theClass, conf);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Socket Factory class not found: " + cnfe);
        }
    }

    public static InetSocketAddress createSocketAddr(String target) {
        return createSocketAddr(target, -1);
    }

    public static InetSocketAddress createSocketAddr(String target, int defaultPort) {
        return createSocketAddr(target, defaultPort, null);
    }

    public static InetSocketAddress createSocketAddr(
            String target, int defaultPort, String configName) {
        String helpText = "";
        if (configName != null) helpText = " (configuration property '" + configName + "')";
        if (target == null)
            throw new IllegalArgumentException("Target address cannot be null." + helpText);
        target = target.trim();
        boolean hasScheme = target.contains("://");
        URI uri = null;
        try {
            uri = hasScheme ? URI.create(target) : URI.create("dummyscheme://" + target);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Does not contain a valid host:port authority: " + target + helpText);
        }
        String host = uri.getHost();
        int port = uri.getPort();
        if (port == -1) port = defaultPort;
        String path = uri.getPath();
        if (host == null || port < 0 || (!hasScheme && path != null && !path.isEmpty()))
            throw new IllegalArgumentException(
                    "Does not contain a valid host:port authority: " + target + helpText);
        return createSocketAddrForHost(host, port);
    }

    public static InetSocketAddress createSocketAddrForHost(String host, int port) {
        InetSocketAddress inetSocketAddress;
        String staticHost = getStaticResolution(host);
        String resolveHost = (staticHost != null) ? staticHost : host;
        try {
            InetAddress iaddr = SecurityUtil.getByName(resolveHost);
            if (staticHost != null) iaddr = InetAddress.getByAddress(host, iaddr.getAddress());
            inetSocketAddress = new InetSocketAddress(iaddr, port);
        } catch (UnknownHostException e) {
            inetSocketAddress = InetSocketAddress.createUnresolved(host, port);
        }
        return inetSocketAddress;
    }

    public static URI getCanonicalUri(URI uri, int defaultPort) {
        String host = uri.getHost();
        if (host == null) return uri;
        String fqHost = canonicalizeHost(host);
        int port = uri.getPort();
        if (host.equals(fqHost) && port != -1) return uri;
        try {
            uri =
                    new URI(
                            uri.getScheme(),
                            uri.getUserInfo(),
                            fqHost,
                            (port == -1) ? defaultPort : port,
                            uri.getPath(),
                            uri.getQuery(),
                            uri.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return uri;
    }

    public static String canonicalizeHost(String host) {
        String fqHost = host;
        try {
            fqHost = SecurityUtil.getByName(host).getCanonicalHostName();
        } catch (UnknownHostException e) {
        }
        return fqHost;
    }

    public static void addStaticResolution(String host, String resolvedName) {
        synchronized (hostToResolved) {
            hostToResolved.put(host, resolvedName);
        }
    }

    public static String getStaticResolution(String host) {
        synchronized (hostToResolved) {
            return hostToResolved.get(host);
        }
    }

    public static List<String[]> getAllStaticResolutions() {
        synchronized (hostToResolved) {
            Set<Map.Entry<String, String>> entries = hostToResolved.entrySet();
            if (entries.size() == 0) return null;
            List<String[]> l = (List) new ArrayList<>(entries.size());
            for (Map.Entry<String, String> e : entries) {
                l.add(new String[] {e.getKey(), e.getValue()});
            }
            return l;
        }
    }

    public static InetSocketAddress getConnectAddress(Server server) {
        return getConnectAddress(server.getListenerAddress());
    }

    public static InetSocketAddress getConnectAddress(InetSocketAddress addr) {
        if (!addr.isUnresolved() && addr.getAddress().isAnyLocalAddress())
            try {
                addr = new InetSocketAddress(InetAddress.getLocalHost(), addr.getPort());
            } catch (UnknownHostException uhe) {
                addr = createSocketAddrForHost("127.0.0.1", addr.getPort());
            }
        return addr;
    }

    public static SocketInputWrapper getInputStream(Socket socket) throws IOException {
        return getInputStream(socket, socket.getSoTimeout());
    }

    public static SocketInputWrapper getInputStream(Socket socket, long timeout)
            throws IOException {
        InputStream stm =
                (socket.getChannel() == null)
                        ? socket.getInputStream()
                        : new SocketInputStream(socket);
        SocketInputWrapper w = new SocketInputWrapper(socket, stm);
        w.setTimeout(timeout);
        return w;
    }

    public static OutputStream getOutputStream(Socket socket) throws IOException {
        return getOutputStream(socket, 0L);
    }

    public static OutputStream getOutputStream(Socket socket, long timeout) throws IOException {
        return (socket.getChannel() == null)
                ? socket.getOutputStream()
                : new SocketOutputStream(socket, timeout);
    }

    public static void connect(Socket socket, SocketAddress address, int timeout)
            throws IOException {
        connect(socket, address, null, timeout);
    }

    public static void connect(
            Socket socket, SocketAddress endpoint, SocketAddress localAddr, int timeout)
            throws IOException {
        if (socket == null || endpoint == null || timeout < 0)
            throw new IllegalArgumentException("Illegal argument for connect()");
        SocketChannel ch = socket.getChannel();
        if (localAddr != null) {
            Class<?> localClass = localAddr.getClass();
            Class<?> remoteClass = endpoint.getClass();
            Preconditions.checkArgument(
                    localClass.equals(remoteClass),
                    "Local address %s must be of same family as remote address %s.",
                    new Object[] {localAddr, endpoint});
            socket.bind(localAddr);
        }
        try {
            if (ch == null) {
                socket.connect(endpoint, timeout);
            } else {
                SocketIOWithTimeout.connect(ch, endpoint, timeout);
            }
        } catch (SocketTimeoutException ste) {
            throw new ConnectTimeoutException(ste.getMessage());
        }
        if (socket.getLocalPort() == socket.getPort()
                && socket.getLocalAddress().equals(socket.getInetAddress())) {
            LOG.info("Detected a loopback TCP socket, disconnecting it");
            socket.close();
            throw new ConnectException(
                    "Localhost targeted connection resulted in a loopback. No daemon is listening on the target port.");
        }
    }

    public static String normalizeHostName(String name) {
        try {
            return InetAddress.getByName(name).getHostAddress();
        } catch (UnknownHostException e) {
            return name;
        }
    }

    public static List<String> normalizeHostNames(Collection<String> names) {
        List<String> hostNames = new ArrayList<>(names.size());
        for (String name : names) hostNames.add(normalizeHostName(name));
        return hostNames;
    }

    public static void verifyHostnames(String[] names) throws UnknownHostException {
        for (String name : names) {
            if (name == null) throw new UnknownHostException("null hostname found");
            URI uri = null;
            try {
                uri = new URI(name);
                if (uri.getHost() == null) uri = new URI("http://" + name);
            } catch (URISyntaxException e) {
                uri = null;
            }
            if (uri == null || uri.getHost() == null)
                throw new UnknownHostException(name + " is not a valid Inet address");
        }
    }

    private static final Pattern ipPortPattern =
            Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(:\\d+)?");

    public static String getHostNameOfIP(String ipPort) {
        if (null == ipPort || !ipPortPattern.matcher(ipPort).matches()) return null;
        try {
            int colonIdx = ipPort.indexOf(':');
            String ip = (-1 == colonIdx) ? ipPort : ipPort.substring(0, ipPort.indexOf(':'));
            return InetAddress.getByName(ip).getHostName();
        } catch (UnknownHostException e) {
            return null;
        }
    }

    public static String getHostname() {
        try {
            return "" + InetAddress.getLocalHost();
        } catch (UnknownHostException uhe) {
            return "" + uhe;
        }
    }

    public static String getHostPortString(InetSocketAddress addr) {
        return addr.getHostName() + ":" + addr.getPort();
    }

    public static InetAddress getLocalInetAddress(String host) throws SocketException {
        if (host == null) return null;
        InetAddress addr = null;
        try {
            addr = SecurityUtil.getByName(host);
            if (NetworkInterface.getByInetAddress(addr) == null) addr = null;
        } catch (UnknownHostException ignore) {
        }
        return addr;
    }

    public static boolean isLocalAddress(InetAddress addr) {
        boolean local = (addr.isAnyLocalAddress() || addr.isLoopbackAddress());
        if (!local)
            try {
                local = (NetworkInterface.getByInetAddress(addr) != null);
            } catch (SocketException e) {
                local = false;
            }
        return local;
    }

    public static IOException wrapException(
            String destHost, int destPort, String localHost, int localPort, IOException exception) {
        if (exception instanceof java.net.BindException)
            return wrapWithMessage(
                    exception,
                    "Problem binding to ["
                            + localHost
                            + ":"
                            + localPort
                            + "] "
                            + exception
                            + ";"
                            + see("BindException"));
        if (exception instanceof ConnectException)
            return wrapWithMessage(
                    exception,
                    "Call From "
                            + localHost
                            + " to "
                            + destHost
                            + ":"
                            + destPort
                            + " failed on connection exception: "
                            + exception
                            + ";"
                            + see("ConnectionRefused"));
        if (exception instanceof UnknownHostException)
            return wrapWithMessage(
                    exception,
                    "Invalid host name: "
                            + getHostDetailsAsString(destHost, destPort, localHost)
                            + exception
                            + ";"
                            + see("UnknownHost"));
        if (exception instanceof SocketTimeoutException)
            return wrapWithMessage(
                    exception,
                    "Call From "
                            + localHost
                            + " to "
                            + destHost
                            + ":"
                            + destPort
                            + " failed on socket timeout exception: "
                            + exception
                            + ";"
                            + see("SocketTimeout"));
        if (exception instanceof java.net.NoRouteToHostException)
            return wrapWithMessage(
                    exception,
                    "No Route to Host from  "
                            + localHost
                            + " to "
                            + destHost
                            + ":"
                            + destPort
                            + " failed on socket timeout exception: "
                            + exception
                            + ";"
                            + see("NoRouteToHost"));
        if (exception instanceof java.io.EOFException)
            return wrapWithMessage(
                    exception,
                    "End of File Exception between "
                            + getHostDetailsAsString(destHost, destPort, localHost)
                            + ": "
                            + exception
                            + ";"
                            + see("EOFException"));
        return (IOException)
                (new IOException(
                                "Failed on local exception: "
                                        + exception
                                        + "; Host Details : "
                                        + getHostDetailsAsString(destHost, destPort, localHost)))
                        .initCause(exception);
    }

    private static String see(String entry) {
        return " For more details see:  http://wiki.apache.org/hadoop/" + entry;
    }

    private static <T extends IOException> T wrapWithMessage(T exception, String msg) {
        Class<? extends Throwable> clazz = (Class) exception.getClass();
        try {
            Constructor<? extends Throwable> ctor =
                    clazz.getConstructor(new Class[] {String.class});
            Throwable t = ctor.newInstance(new Object[] {msg});
            return (T) t.initCause((Throwable) exception);
        } catch (Throwable e) {
            LOG.warn(
                    "Unable to wrap exception of type "
                            + clazz
                            + ": it has no (String) constructor",
                    e);
            return exception;
        }
    }

    private static String getHostDetailsAsString(String destHost, int destPort, String localHost) {
        StringBuilder hostDetails = new StringBuilder(27);
        hostDetails.append("local host is: ").append(quoteHost(localHost)).append("; ");
        hostDetails
                .append("destination host is: ")
                .append(quoteHost(destHost))
                .append(":")
                .append(destPort)
                .append("; ");
        return hostDetails.toString();
    }

    private static String quoteHost(String hostname) {
        return (hostname != null) ? ("\"" + hostname + "\"") : "(unknown)";
    }

    public static boolean isValidSubnet(String subnet) {
        try {
            new SubnetUtils(subnet);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    private static void addMatchingAddrs(
            NetworkInterface nif, SubnetUtils.SubnetInfo subnetInfo, List<InetAddress> addrs) {
        Enumeration<InetAddress> ifAddrs = nif.getInetAddresses();
        while (ifAddrs.hasMoreElements()) {
            InetAddress ifAddr = ifAddrs.nextElement();
            if (subnetInfo.isInRange(ifAddr.getHostAddress())) addrs.add(ifAddr);
        }
    }

    public static List<InetAddress> getIPs(String subnet, boolean returnSubinterfaces) {
        Enumeration<NetworkInterface> nifs;
        List<InetAddress> addrs = new ArrayList<>();
        SubnetUtils.SubnetInfo subnetInfo = (new SubnetUtils(subnet)).getInfo();
        try {
            nifs = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            LOG.error("Unable to get host interfaces", e);
            return addrs;
        }
        while (nifs.hasMoreElements()) {
            NetworkInterface nif = nifs.nextElement();
            addMatchingAddrs(nif, subnetInfo, addrs);
            if (!returnSubinterfaces) continue;
            Enumeration<NetworkInterface> subNifs = nif.getSubInterfaces();
            while (subNifs.hasMoreElements())
                addMatchingAddrs(subNifs.nextElement(), subnetInfo, addrs);
        }
        return addrs;
    }

    public static int getFreeSocketPort() {
        int port = 0;
        try {
            ServerSocket s = new ServerSocket(0);
            port = s.getLocalPort();
            s.close();
            return port;
        } catch (IOException e) {
            return port;
        }
    }
}
