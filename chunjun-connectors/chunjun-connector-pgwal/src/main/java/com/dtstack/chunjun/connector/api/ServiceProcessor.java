package com.dtstack.chunjun.connector.api;

import java.sql.SQLException;

public interface ServiceProcessor<T, OUT> extends java.io.Closeable {

    void init(java.util.Map<String, Object> param) throws SQLException;

    DataProcessor<OUT> dataProcessor();

    void process(Context context) throws SQLException;

    interface Context {

        <K, V> V get(K key, Class<V> type);

        <V> V get(Class<V> data);

        <K, V> void set(K key, V data);

        <K> boolean contains(K name);
    }
}
