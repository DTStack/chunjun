package com.dtstack.flinkx.pulsar.client;

import com.dtstack.flinkx.pulsar.format.PulsarInputFormat;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * The consumer of PulsarReader
 * <p>
 * Company: www.dtstack.com
 *
 * @author fengjiangtao_yewu@cmss.chinamobile.com 2021-3-23
 */
public class FlinkxPulsarConsumer {

    protected Properties props;
    protected FlinkxPulsarClient client;

    protected ExecutorService executor = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory
            .Builder()
            .namingPattern("PulsarConsumerThread-" + Thread.currentThread().getName())
            .daemon(true)
            .build());

    public FlinkxPulsarConsumer() {
    }

    public FlinkxPulsarConsumer(Properties props) {
        this.props = props;
    }

    public FlinkxPulsarConsumer createClient(PulsarInputFormat format) {
        client = new FlinkxPulsarClient(format);
        return this;
    }

    public void execute() {
        executor.execute(client);
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
