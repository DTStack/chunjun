package com.dtstack.chunjun.connector.mongodb.util;

import org.bson.json.Converter;
import org.bson.json.StrictJsonWriter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateTimeConverter implements Converter<Long> {
    @Override
    public void convert(Long value, StrictJsonWriter writer) {
        String dateTimeStr =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withZone(ZoneId.systemDefault())
                        .format(Instant.ofEpochMilli(value));
        writer.writeString(dateTimeStr);
    }
}
