package com.dtstack.chunjun.connector.api;

import org.apache.flink.table.api.TableEnvironment;

public interface ExtendTableEnvironment extends TableEnvironment {

    static ExtendTableEnvironment create(CDCSettings settings) {
        return ExtendTableEnvironmentImpl.create(settings);
    }
}
