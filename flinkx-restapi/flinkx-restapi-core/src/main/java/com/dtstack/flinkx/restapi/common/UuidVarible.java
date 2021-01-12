package com.dtstack.flinkx.restapi.common;

import java.util.UUID;

public class UuidVarible implements Paramitem<String> {


    public UuidVarible() {
    }

    @Override
    public String getValue(RestContext restContext) {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getName() {
        return "currentTime";
    }

}
