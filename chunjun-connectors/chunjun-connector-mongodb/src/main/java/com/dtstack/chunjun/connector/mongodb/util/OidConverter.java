package com.dtstack.chunjun.connector.mongodb.util;

import org.bson.json.Converter;
import org.bson.json.StrictJsonWriter;
import org.bson.types.ObjectId;

/**
 * Description: date: 2023/3/2 21:26
 *
 * @author XiongZhibin
 */
public class OidConverter implements Converter<ObjectId> {
    @Override
    public void convert(ObjectId oid, StrictJsonWriter writer) {
        writer.writeString(oid.toString());
    }
}
