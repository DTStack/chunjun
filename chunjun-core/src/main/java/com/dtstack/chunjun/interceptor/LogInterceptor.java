package com.dtstack.chunjun.interceptor;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LogInterceptor.class);

    @Override
    public void init(Configuration configuration) {
        if (LOG.isDebugEnabled()) {
            LOG.info("configuration: ");
            configuration
                    .keySet()
                    .forEach(
                            key ->
                                    LOG.debug(
                                            "key : {} -> value : {}",
                                            key,
                                            configuration.toMap().get(key)));
        }
    }

    @Override
    public void pre(Context context) {
        if (LOG.isDebugEnabled()) {
            for (String key : context) {
                LOG.debug("context key : {} -> value : {} ", key, context.get(key));
            }
        }
    }

    @Override
    public void post(Context context) {
        if (LOG.isDebugEnabled()) {
            for (String key : context) {
                LOG.debug("context key : {} -> value : {} ", key, context.get(key));
            }
        }
    }
}
