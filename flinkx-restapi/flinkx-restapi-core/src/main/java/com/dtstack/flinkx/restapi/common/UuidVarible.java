package com.dtstack.flinkx.restapi.common;

import java.util.UUID;

public class UuidVarible implements Paramitem {

//    private ParamDefinition paramDefinition;

    public UuidVarible() {
    }

//    public UuidVarible(ParamDefinition paramDefinition) {
//        this.paramDefinition = paramDefinition;
//    }

    @Override
    public Object getValue() {
        return UUID.randomUUID();
    }

//    @Override
//    public ParamDefinition getParamDefinition() {
//        return paramDefinition;
//    }


}
