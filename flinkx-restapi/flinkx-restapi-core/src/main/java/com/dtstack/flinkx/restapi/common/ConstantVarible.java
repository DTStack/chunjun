package com.dtstack.flinkx.restapi.common;

public class ConstantVarible<T> implements Paramitem {
    private final T object;
    private final String name;
//    private ParamDefinition paramDefinition;

    public ConstantVarible(T object, String name) {
        this.object = object;
        this.name = name;
//        this.paramDefinition = paramDefinition;
    }

    @Override
    public T getValue(RestContext restContext) {
        return object;
    }

    @Override
    public String getName() {
        return name;
    }

//    @Override
//    public ParamDefinition getParamDefinition() {
//        return paramDefinition;
//    }
}
