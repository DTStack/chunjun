/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema.Names;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;

/** @author liuliu 2022/3/1 */
public class Schemas {
    static final JsonFactory FACTORY = new JsonFactory();
    static final ObjectMapper MAPPER;

    public Schemas() {}

    public static String toString(Schema schema, Collection<Schema> schemas) {
        return toString(schema, schemas, false);
    }

    public static String toString(Schema schema, Collection<Schema> schemas, boolean pretty) {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator gen = FACTORY.createGenerator(writer);
            if (pretty) {
                gen.useDefaultPrettyPrinter();
            }

            Names names = new Names();
            if (schemas != null) {
                Iterator var6 = schemas.iterator();

                while (var6.hasNext()) {
                    Schema s = (Schema) var6.next();
                    names.add(s);
                }
            }

            schema.toJson(names, gen);
            gen.flush();
            return writer.toString();
        } catch (IOException var8) {
            throw new AvroRuntimeException(var8);
        }
    }

    static {
        MAPPER = new ObjectMapper(FACTORY);
        FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
        FACTORY.setCodec(MAPPER);
    }
}
