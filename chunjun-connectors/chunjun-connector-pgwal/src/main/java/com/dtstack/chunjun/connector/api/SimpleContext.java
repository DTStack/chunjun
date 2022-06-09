package com.dtstack.chunjun.connector.api;

import java.util.HashMap;
import java.util.Map;

public class SimpleContext implements ServiceProcessor.Context {

    private Map<Object, Object> params = new HashMap<>();

    @Override
    public <K, V> V get(K key, Class<V> type) {
        Object object = params.get(key);
        assert type.isAssignableFrom(object.getClass());
        return (V) object;
    }

    @Override
    public <V> V get(Class<V> data) {
        return (V)
                params.values().stream()
                        .filter(value -> data.isAssignableFrom(value.getClass()))
                        .findAny()
                        .get();
    }

    @Override
    public <K, V> void set(K key, V data) {
        params.put(key, data);
    }

    @Override
    public <K> boolean contains(K name) {
        return params.containsKey(name);
    }
}
