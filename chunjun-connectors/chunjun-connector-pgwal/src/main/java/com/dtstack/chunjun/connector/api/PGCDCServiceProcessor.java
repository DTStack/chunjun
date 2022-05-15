package com.dtstack.chunjun.connector.api;

import com.dtstack.chunjun.connector.pgwal.conf.PGWalConf;
import com.dtstack.chunjun.connector.pgwal.converter.PGWalColumnConverter;
import com.dtstack.chunjun.connector.pgwal.util.PGUtil;
import com.dtstack.chunjun.connector.pgwal.util.PgDecoder;
import com.dtstack.chunjun.util.RetryUtil;

import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PGCDCServiceProcessor extends DatabaseBaseRichInputFormat<RowData, RowData>
        implements ServiceProcessor<RowData, RowData> {

    private static Gson gson = new Gson();
    private Map<String, Object> param;
    private PGWalConf conf;

    private PgConnection conn;
    private PGReplicationStream stream;
    private PgDecoder decoder;
    private PGWalColumnConverter converter;

    private volatile boolean running;

    private ByteBuffer buffer;

    @Override
    public void init(Map<String, Object> param) throws SQLException {
        this.param = param;

        this.conn =
                RetryUtil.executeWithRetry(
                        () ->
                                (PgConnection)
                                        PGUtil.getConnection(
                                                conf.jdbcUrl, conf.username, conf.password),
                        3,
                        2000,
                        true);
        converter = new PGWalColumnConverter(conf.pavingData, conf.pavingData);
        decoder = new PgDecoder(PGUtil.queryTypes(conn), conf);
        ChainedLogicalStreamBuilder builder =
                conn.getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(conf.getSlotName())
                        // 协议版本。当前仅支持版本1
                        .withSlotOption("proto_version", "1")
                        // 逗号分隔的要订阅的发布名称列表（接收更改）。 单个发布名称被视为标准对象名称，并可根据需要引用
                        .withSlotOption("publication_names", PGUtil.PUBLICATION_NAME)
                        .withStatusInterval(conf.getStatusInterval(), TimeUnit.SECONDS);
        long lsn = (Long) param.get("lsn");
        if (lsn != 0) {
            builder.withStartPosition(LogSequenceNumber.valueOf(lsn));
        }
        stream = builder.start();

        stream.forceUpdateStatus();
        LOG.info("init PGReplicationStream successfully...");
    }

    @Override
    public void process(Context context) throws SQLException {
        while (running) {
            try {
                buffer = stream.readPending();
                context.set("data", buffer);
                dataProcessor().process(context);
            } catch (IOException e) {
                dataProcessor().processException(e);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public DataProcessor<RowData> dataProcessor() {
        return new PGDataProcessor(param);
    }

    @Override
    public void close() throws IOException {
        try {
            stream.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
