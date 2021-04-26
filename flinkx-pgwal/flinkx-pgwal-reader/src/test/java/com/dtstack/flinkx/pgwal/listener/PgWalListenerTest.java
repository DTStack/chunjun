package com.dtstack.flinkx.pgwal.listener;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.pgwal.format.PgWalInputFormat;
import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.postgresql.core.ReplicationProtocol;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.PGReplicationConnectionImpl;
import org.postgresql.replication.fluent.ReplicationStreamBuilder;
import org.postgresql.replication.fluent.logical.LogicalStreamBuilder;

import java.util.*;

import static org.powermock.api.mockito.PowerMockito.*;

public class PgWalListenerTest {
    private PgWalListener listener;

    @BeforeClass
    @Ignore
    public void setUp() throws Exception {
        PgWalInputFormat format = spy(new PgWalInputFormat());
//        when(format.getRuntimeContext()).thenReturn(new MockRuntimeContext(new LifeCycleTestInputFormat(), 1, null));
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> jobParams = new HashMap<>();
        jobParams.put("setting", new HashMap<>());
        Map<String, Object> job = new HashMap<>(jobParams);
        List<Map<String, Object>> contents = new ArrayList<>();
        Map<String, Object> content = new HashMap<>();
        Map<String, Object> readParams = new HashMap<>();

        content.put("reader", readParams);
        Map<String, Object> readParam = new HashMap<>();
        readParams.put("parameter", readParam);
        readParam.put("username", "dummy");
        readParam.put("password", "dummy");
        readParam.put("url", "dummy");
        readParam.put("databaseName", "dummy");
        readParam.put("cat", "dummy");
        readParam.put("tableList", Lists.newArrayList("a"));

        Map<String, Object> writerParams = new HashMap<>();


        writerParams.put("parameter", new HashMap<>());
        content.put("writer", writerParams);
        contents.add(content);
        job.put("content", contents);
        params.put("job", job);

        DataTransferConfig config = new DataTransferConfig(params);
        format.setDataTransferConfig(config);
        format.setTableList(Lists.newArrayList("a"));
        format.setCat("a");
        PgConnection connection = mock(PgConnection.class);
        PGReplicationConnectionImpl replicationConnection = spy(new PGReplicationConnectionImpl(connection));
        when(connection.getReplicationAPI()).thenReturn(replicationConnection);
        ReplicationStreamBuilder streamBuilder = spy(new ReplicationStreamBuilder(connection));
        when(streamBuilder.logical()).thenReturn(new LogicalStreamBuilder(options -> {
            ReplicationProtocol protocol = connection.getReplicationProtocol();
            return protocol.startLogical(options);
        }));
        when(replicationConnection.replicationStream()).thenReturn(streamBuilder);
//        QueryExecutorImpl queryExecutor = new QueryExecutorImpl(newStream, user, database,
//                cancelSignalTimeout, info);
//        V3ReplicationProtocol protocol = new V3ReplicationProtocol(queryExecutor, );
        format.setConnection(connection);
        listener = new PgWalListener(format);
        format.openInputFormat();
    }

    public void tearDown() throws Exception {
        listener = null;
    }

    public void testInit() {
    }
    @Ignore
    public void testTestRun() {
        listener.run();
    }
}