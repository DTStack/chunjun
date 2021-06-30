/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.pgwal.source;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.connectors.postgres.table.PostgresValueValidator;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.dtstack.flinkx.converter.RawTypeConverter;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.pgwal.conf.PGWalConf;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.util.JsonUtil;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.JdbcCatalogValidator;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.logical.RowType;

import org.postgresql.Driver;

import javax.annotation.Nullable;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 */
public class PgwalSourceFactory extends SourceFactory {

    private final PGWalConf conf;
    private ExecutionConfig config = new ExecutionConfig();
    private DebeziumSourceFunction<RowData> sourceFunction;

    @SuppressWarnings("all")
    public PgwalSourceFactory(SyncConf config, StreamExecutionEnvironment env) throws IllegalAccessException, URISyntaxException, TableNotExistException, MalformedURLException {
        super(config, env);
        conf = JsonUtil.toObject(JsonUtil.toJson(config.getReader().getParameter()), PGWalConf.class);
        conf.setColumn(config.getReader().getFieldList());
        super.initFlinkxCommonConf(conf);

        Configuration configuration = buildConfiguration();
        Catalog catalog = new JdbcCatalogFactory().createCatalog(configuration.getString("database", ""), configuration.toMap());
        CatalogBaseTable table = catalog.getTable(new ObjectPath(configuration.getString("database", ""), configuration.getString("table", "")));
        TableSchema schema = table.getSchema();

        RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo =
                (TypeInformation<RowData>) new ScanRuntimeProviderContext().createTypeInformation(schema.toRowDataType());
        configuration.setString("heartbeat.interval.ms", String.valueOf(configuration.getInteger("heartbeat.interval.ms", 1000)));
        sourceFunction = PostgreSQLSource.<RowData>builder()
                .hostname(configuration.getString("host", ""))
                .port(configuration.getInteger("port", 8020))
                .database(configuration.getString("database", ""))
                .username(configuration.getString("username", ""))
                .password(configuration.getString("password", ""))
                .decodingPluginName("pgoutput")
                .schemaList(configuration.getString("schema", ""))
                .tableList(configuration.getString("table", ""))
                .deserializer(new RowDataDebeziumDeserializeSchema(
                        rowType,
                        typeInfo,
                        new PostgresValueValidator(configuration.getString("schema", "public"), configuration.getString("table", "")),
                        ZoneId.of("UTC")))
                .debeziumProperties(toProperties(configuration))
                .build();
    }

    private Properties toProperties(Configuration configuration) {
        Properties properties = new Properties();
        configuration.keySet().forEach(key -> properties.setProperty(key, configuration.getString(key, null)));
        return properties;
    }

    private Configuration buildConfiguration() {
        Configuration configuration = new Configuration();
        Properties properties = new Properties();

        properties = Driver.parseURL(conf.getJdbcUrl(), properties);
        final Map<String, String> urlParam = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            urlParam.put(key, value);
        }
        configuration = configuration.fromMap(urlParam);

        configuration.setString("database", conf.getDatabase());
        configuration.setString("username", conf.getUsername());
        configuration.setString("password", conf.getPassword());
        configuration.setString("table", conf.getTables().get(0));
        configuration.setString("schema", "public");
        configuration.setString("host", properties.getProperty("PGHOST", ""));
        configuration.setString("port", (properties.getProperty("PGPORT", "5432")));


        configuration.setString(JdbcCatalogValidator.CATALOG_DEFAULT_DATABASE, configuration.getString("database", ""));
        configuration.setString(JdbcCatalogValidator.CATALOG_JDBC_BASE_URL, buildBaseURI(configuration));
        configuration.setString(JdbcCatalogValidator.CATALOG_JDBC_USERNAME, conf.getUsername());
        configuration.setString(JdbcCatalogValidator.CATALOG_JDBC_PASSWORD, conf.getPassword());
        configuration.setString(CatalogDescriptorValidator.CATALOG_TYPE, JdbcCatalogValidator.CATALOG_TYPE_VALUE_JDBC);
        return configuration;
    }

    private String buildBaseURI(Configuration configuration) {
        return "jdbc:postgresql://" + configuration.getString("host", "") + ":"  + configuration.getString("port", "") + "/";
    }

    private <OUT> DataStreamSource<OUT> addSource(
            final SourceFunction<OUT> function,
            final String sourceName,
            @Nullable final TypeInformation<OUT> typeInfo,
            final Boundedness boundedness) {
        checkNotNull(function);
        checkNotNull(sourceName);
        checkNotNull(boundedness);

        TypeInformation<OUT> resolvedTypeInfo =
                getTypeInfo(function, sourceName, SourceFunction.class, typeInfo);

        boolean isParallel = function instanceof ParallelSourceFunction;

        clean(function);

        final StreamSource<OUT, ?> sourceOperator = new StreamSource<>(function);
        return new DataStreamSource<>(
                this.env, resolvedTypeInfo, sourceOperator, isParallel, sourceName, boundedness);
    }

    public <F> F clean(F f) {
        if (config.isClosureCleanerEnabled()) {
            ClosureCleaner.clean(f, config.getClosureCleanerLevel(), true);
        }
        ClosureCleaner.ensureSerializable(f);
        return f;
    }

    @SuppressWarnings("all")
    private <OUT, T extends TypeInformation<OUT>> T getTypeInfo(
            Object source,
            String sourceName,
            Class<?> baseSourceClass,
            TypeInformation<OUT> typeInfo) {
        TypeInformation<OUT> resolvedTypeInfo = typeInfo;
        if (resolvedTypeInfo == null && source instanceof ResultTypeQueryable) {
            resolvedTypeInfo = ((ResultTypeQueryable<OUT>) source).getProducedType();
        }
        if (resolvedTypeInfo == null) {
            try {
                resolvedTypeInfo =
                        TypeExtractor.createTypeInfo(
                                baseSourceClass, source.getClass(), 0, null, null);
            } catch (final InvalidTypesException e) {
                resolvedTypeInfo = (TypeInformation<OUT>) new MissingTypeInfo(sourceName, e);
            }
        }
        return (T) resolvedTypeInfo;
    }

    @Override
    public DataStream<RowData> createSource() {
        return addSource(sourceFunction, "pg-cdc", null, Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
       return null;
    }
}
