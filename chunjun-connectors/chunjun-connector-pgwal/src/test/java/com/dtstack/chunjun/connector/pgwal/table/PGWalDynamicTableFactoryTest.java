package com.dtstack.chunjun.connector.pgwal.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalogUtils;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.fail;

public class PGWalDynamicTableFactoryTest {

    @Test
    public void testStartStream() throws TableNotExistException {
        PGWalDynamicTableFactory dynamicTableFactory = new PGWalDynamicTableFactory();
        String catalogName =
                (String) getOptionByName(dynamicTableFactory.requiredOptions(), "catalog");
        String defaultDatabase =
                (String) getOptionByName(dynamicTableFactory.requiredOptions(), "database");
        String username =
                (String) getOptionByName(dynamicTableFactory.requiredOptions(), "username");
        String pwd = (String) getOptionByName(dynamicTableFactory.requiredOptions(), "password");
        String baseUrl = (String) getOptionByName(dynamicTableFactory.requiredOptions(), "url");
        AbstractJdbcCatalog catalog =
                JdbcCatalogUtils.createCatalog(
                        catalogName, defaultDatabase, username, pwd, baseUrl);
        boolean isTempTable = true;
        DynamicTableSource tableSource =
                FactoryUtil.createTableSource(
                        catalog,
                        ObjectIdentifier.of(catalogName, defaultDatabase, ""),
                        (CatalogTable) catalog.getTable(new ObjectPath(defaultDatabase, "")),
                        new Configuration(),
                        ClassLoader.getSystemClassLoader(),
                        isTempTable);

        fail();
    }

    @Test
    public void testStreamDSL() {
        StreamConfiguration configuration = new StreamConfiguration();
        StreamEnvironment environment = StreamEnvironment.create(configuration);
        //        environment.from()
    }

    private Object getOptionByName(Set<ConfigOption<?>> optionSet, String name) {
        if (optionSet == null) return null;
        return optionSet.stream()
                .filter(option -> option.key().equals(name))
                .findAny()
                .get()
                .defaultValue();
    }
}
