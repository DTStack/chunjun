package com.dtstack.flinkx.restapi.common;

import java.util.UUID;

public class UuidVarible implements Paramitem<String> {


    public UuidVarible() {
    }

//    public UuidVarible(ParamDefinition paramDefinition) {
//        this.paramDefinition = paramDefinition;
//    }

    @Override
    public String getValue(RestContext restContext) {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getName() {
        return "currentTime";
    }

//    @Override
//    public ParamDefinition getParamDefinition() {
//        return paramDefinition;
//    }


}
