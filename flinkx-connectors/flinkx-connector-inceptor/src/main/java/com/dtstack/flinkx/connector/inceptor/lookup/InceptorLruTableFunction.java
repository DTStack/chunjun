package com.dtstack.flinkx.connector.inceptor.lookup;

import com.dtstack.flinkx.connector.inceptor.conf.InceptorConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.security.KerberosUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.dtstack.flinkx.connector.jdbc.options.JdbcLookupOptions.*;

/**
 * @author dujie @Description TODO
 * @createTime 2022-01-20 04:50:00
 */
public class InceptorLruTableFunction extends JdbcLruTableFunction {
    private final InceptorConf inceptorConf;
    private UserGroupInformation ugi;

    public InceptorLruTableFunction(
            InceptorConf inceptorConf,
            JdbcDialect jdbcDialect,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(inceptorConf, jdbcDialect, lookupConf, fieldNames, keyNames, rowType);
        this.inceptorConf = inceptorConf;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.ugi = KerberosUtil.loginAndReturnUgi(inceptorConf.getHadoopConfig());
    }

    @Override
    protected void asyncQueryData(
            CompletableFuture<Collection<RowData>> future,
            SQLClient rdbSqlClient,
            AtomicLong failCounter,
            AtomicBoolean finishFlag,
            CountDownLatch latch,
            Object... keys) {
        ugi.doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        doAsyncQueryData(
                                future, rdbSqlClient, failCounter, finishFlag, latch, keys);
                        return null;
                    }
                });
    }

    /**
     * get jdbc connection
     *
     * @return
     */
    @Override
    public JsonObject createJdbcConfig(Map<String, Object> druidConfMap) {
        JsonObject jdbcConfig = super.createJdbcConfig(druidConfMap);
        JsonObject clientConfig = new JsonObject();

        Iterator<Map.Entry<String, Object>> iterator = jdbcConfig.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();

            if (Objects.nonNull(next.getValue())) {
                clientConfig.put(next.getKey(), next.getValue());
            }
        }
        return clientConfig;
    }
}
