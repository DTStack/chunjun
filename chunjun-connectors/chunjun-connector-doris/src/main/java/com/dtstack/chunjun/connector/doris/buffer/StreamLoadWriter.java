package com.dtstack.chunjun.connector.doris.buffer;

import com.dtstack.chunjun.connector.doris.options.DorisConfig;

import com.alibaba.fastjson.JSON;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_SOCKET_TIMEOUT_MS_DEFAULT;

public class StreamLoadWriter implements BufferFlusher {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadWriter.class);
    private static final String RESULT_FAILED = "Fail";
    private static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    private static final String LAEBL_STATE_VISIBLE = "VISIBLE";
    private static final String LAEBL_STATE_COMMITTED = "COMMITTED";
    private static final String RESULT_LABEL_PREPARE = "PREPARE";
    private static final String RESULT_LABEL_ABORTED = "ABORTED";
    private static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";
    private static final int ERROR_LOG_MAX_LENGTH = 3000;

    private final DorisConfig dorisConf;
    private long pos;

    private final String database;
    private final String table;
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(("yyyyMMdd_HHmmss"));
    private HttpClientBuilder httpclientBuilder;

    public StreamLoadWriter(DorisConfig dorisConf) {
        this.dorisConf = dorisConf;
        this.database = dorisConf.getDatabase();
        this.table = dorisConf.getTable();
        int socketTimeout =
                dorisConf.getLoadConfig().getSocketTimeOutMs() == null
                        ? DORIS_SOCKET_TIMEOUT_MS_DEFAULT
                        : dorisConf.getLoadConfig().getSocketTimeOutMs();

        int connectTimeout =
                dorisConf.getLoadConfig().getRequestConnectTimeoutMs() == null
                        ? DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT
                        : dorisConf.getLoadConfig().getRequestConnectTimeoutMs();

        RequestConfig config =
                RequestConfig.custom()
                        .setSocketTimeout(socketTimeout)
                        .setConnectTimeout(connectTimeout)
                        .build();
        this.httpclientBuilder =
                HttpClients.custom()
                        .setDefaultRequestConfig(config)
                        .setRedirectStrategy(
                                new DefaultRedirectStrategy() {
                                    @Override
                                    protected boolean isRedirectable(String method) {
                                        return true;
                                    }
                                });
    }

    @Override
    public void write(InputStream inputStream, int length) throws Exception {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the hosts in `load_url` could be connected.");
        }
        String lable = initBatchLabel();
        String loadUrl = host + "/api/" + database + "/" + table + "/_stream_load";
        LOG.info("Start to join batch data: label[{}].", lable);
        Map<String, Object> loadResult = doHttpPut(loadUrl, lable, inputStream, length);
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException(
                    "Unable to flush data to StarRocks: unknown result status, usually caused by: 1.authorization or permission related problems. 2.Wrong column_separator or row_delimiter. 3.Column count exceeded the limitation.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
        }
        if (RESULT_FAILED.equals(loadResult.get(keyStatus))) {
            Map<String, String> logMap = new HashMap<>();
            if (loadResult.containsKey("ErrorURL")) {
                logMap.put("streamLoadErrorLog", getErrorLog((String) loadResult.get("ErrorURL")));
            }
            throw new DorisStreamLoadFailedException(
                    String.format(
                            "Failed to flush data to StarRocks, Error " + "response: \n%s\n%s\n",
                            JSON.toJSONString(loadResult), JSON.toJSONString(logMap)),
                    loadResult);
        } else if (RESULT_LABEL_EXISTED.equals(loadResult.get(keyStatus))) {
            LOG.error(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
            // has to block-checking the state to get the final result
            checkLabelState(host, lable);
        }
    }

    @Override
    public void write(Supplier<InputStream> supplier, int length) throws Exception {
        try (InputStream inputStream = supplier.get()) {
            write(inputStream, length);
        }
    }

    private Map<String, Object> doHttpPut(
            String loadUrl, String label, InputStream inputStream, int length) throws IOException {
        LOG.info(
                "Executing stream load to: '{}', size: '{}', thread: {}",
                loadUrl,
                length,
                Thread.currentThread().getId());
        HttpPut httpPut = new HttpPut(loadUrl);

        httpPut.setHeader(
                "columns",
                dorisConf.getColumn().stream()
                        .map(i -> String.format("`%s`", i.getName()))
                        .collect(Collectors.joining(",")));

        if (!httpPut.containsHeader("timeout")) {
            httpPut.setHeader("timeout", "60");
        }

        if (dorisConf.isEnableDelete()) {
            httpPut.setHeader("hidden_columns", DorisSinkOP.COLUMN_KEY);
        }

        httpPut.setHeader("Expect", "100-continue");
        httpPut.setHeader("ignore_json_size", "true");
        httpPut.setHeader("strip_outer_array", "true");
        httpPut.setHeader("partial_update", String.valueOf(dorisConf.isPartialUpdate()));
        httpPut.setHeader("strict_mode", String.valueOf(dorisConf.isStrictMode()));
        httpPut.setHeader("format", "json");
        httpPut.setHeader("label", label);
        httpPut.setHeader(
                "Authorization",
                getBasicAuthHeader(dorisConf.getUsername(), dorisConf.getPassword()));
        httpPut.setEntity(new InputStreamEntity(inputStream, length));
        httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());

        try (CloseableHttpClient httpclient = httpclientBuilder.build()) {
            CloseableHttpResponse resp = httpclient.execute(httpPut);
            HttpEntity respEntity = getHttpEntity(resp);
            if (respEntity == null) return null;
            Map<String, Object> result =
                    (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
            resp.close();
            return result;
        }
    }

    private void checkLabelState(String host, String label) throws IOException {
        int idx = 0;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            } catch (InterruptedException ex) {
                break;
            }
            HttpGet httpGet =
                    new HttpGet(
                            host
                                    + "/api/"
                                    + dorisConf.getDatabase()
                                    + "/get_load_state?label="
                                    + label);
            httpGet.setHeader(
                    "Authorization",
                    getBasicAuthHeader(dorisConf.getUsername(), dorisConf.getPassword()));
            httpGet.setHeader("Connection", "close");

            try (CloseableHttpClient httpclient = httpclientBuilder.build()) {
                CloseableHttpResponse resp = httpclient.execute(httpGet);
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) {
                    throw new DorisStreamLoadFailedException(
                            String.format(
                                    "Failed to flush data to StarRocks, Error "
                                            + "could not get the final state of label[%s].\n",
                                    label),
                            null);
                }
                Map<String, Object> result =
                        (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
                String labelState = (String) result.get("state");
                if (null == labelState) {
                    throw new DorisStreamLoadFailedException(
                            String.format(
                                    "Failed to flush data to StarRocks, Error "
                                            + "could not get the final state of label[%s]. response[%s]\n",
                                    label, EntityUtils.toString(respEntity)),
                            null);
                }
                LOG.info(String.format("Checking label[%s] state[%s]\n", label, labelState));
                resp.close();
                switch (labelState) {
                    case LAEBL_STATE_VISIBLE:
                    case LAEBL_STATE_COMMITTED:
                        return;
                    case RESULT_LABEL_PREPARE:
                        continue;
                    case RESULT_LABEL_ABORTED:
                        throw new DorisStreamLoadFailedException(
                                String.format(
                                        "Failed to flush data to StarRocks, Error "
                                                + "label[%s] state[%s]\n",
                                        label, labelState),
                                null,
                                true);
                    case RESULT_LABEL_UNKNOWN:
                    default:
                        throw new DorisStreamLoadFailedException(
                                String.format(
                                        "Failed to flush data to StarRocks, Error "
                                                + "label[%s] state[%s]\n",
                                        label, labelState),
                                null);
                }
            }
        }
    }

    public String initBatchLabel() {
        String formatDate = LocalDateTime.now().format(dateTimeFormatter);
        return String.format(
                "flinkx_connector_%s_%s",
                formatDate, UUID.randomUUID().toString().replaceAll("-", ""));
    }

    private String getErrorLog(String errorUrl) {
        if (errorUrl == null || !errorUrl.startsWith("http")) {
            return null;
        }
        try {
            HttpGet httpGet = new HttpGet(errorUrl);
            try (CloseableHttpClient httpclient = httpclientBuilder.build()) {
                CloseableHttpResponse resp = httpclient.execute(httpGet);
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) {
                    return null;
                }
                String errorLog = EntityUtils.toString(respEntity);
                if (errorLog != null && errorLog.length() > ERROR_LOG_MAX_LENGTH) {
                    errorLog = errorLog.substring(0, ERROR_LOG_MAX_LENGTH);
                }
                resp.close();
                return errorLog;
            }
        } catch (Exception e) {
            LOG.warn("Failed to get error log.", e);
            return "Failed to get error log: " + e.getMessage();
        }
    }

    private HttpEntity getHttpEntity(CloseableHttpResponse resp) {
        int code = resp.getStatusLine().getStatusCode();
        if (200 != code) {
            LOG.warn("Request failed with code:{}", code);
            return null;
        }
        HttpEntity respEntity = resp.getEntity();
        if (null == respEntity) {
            LOG.warn("Request failed with empty response.");
            return null;
        }
        return respEntity;
    }

    private String getAvailableHost() {
        List<String> hostList = dorisConf.getFeNodes();
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = "http://" + hostList.get((int) (pos % hostList.size()));
            if (tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(dorisConf.getLoadConfig().getHttpCheckTimeoutMs());
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    public void close() throws IOException {}

    private static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }
}
