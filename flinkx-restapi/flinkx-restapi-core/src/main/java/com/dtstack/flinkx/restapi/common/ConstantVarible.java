package com.dtstack.flinkx.restapi.common;

public class ConstantVarible implements Paramitem {
    private final Object object;
//    private ParamDefinition paramDefinition;

    public ConstantVarible(Object object ) {
        this.object = object;
//        this.paramDefinition = paramDefinition;
    }

    @Override
    public Object getValue() {
        return object;
    }

//    @Override
//    public ParamDefinition getParamDefinition() {
//        return paramDefinition;
//    }
}
