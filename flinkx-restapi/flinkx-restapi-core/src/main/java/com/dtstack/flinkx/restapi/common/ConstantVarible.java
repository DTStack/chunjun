package com.dtstack.flinkx.restapi.common;

public class ConstantVarible<T> implements Paramitem {
    private final T object;
    private final String name;

    public ConstantVarible(T object, String name) {
        this.object = object;
        this.name = name;
    }

    @Override
    public T getValue(RestContext restContext) {
        return object;
    }

    @Override
    public String getName() {
        return name;
    }

}
