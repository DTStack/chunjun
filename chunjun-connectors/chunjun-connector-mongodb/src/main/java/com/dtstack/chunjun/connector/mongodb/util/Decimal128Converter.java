package com.dtstack.chunjun.connector.mongodb.util;

import org.bson.json.Converter;
import org.bson.json.StrictJsonWriter;
import org.bson.types.Decimal128;

public class Decimal128Converter implements Converter<Decimal128> {
    @Override
    public void convert(Decimal128 value, StrictJsonWriter writer) {
        writer.writeString(value.toString());
    }
}
