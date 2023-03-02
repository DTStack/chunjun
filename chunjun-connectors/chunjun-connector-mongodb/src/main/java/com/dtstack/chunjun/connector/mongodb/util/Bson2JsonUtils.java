package com.dtstack.chunjun.connector.mongodb.util;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;

public class Bson2JsonUtils {
    public static String toJson(Document document){
        JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
                .decimal128Converter(new Decimal128Converter())
                .dateTimeConverter(new DateTimeConverter())
                .objectIdConverter(new OidConverter())
                .build();

        return document.toJson(jsonWriterSettings);
    }
}
