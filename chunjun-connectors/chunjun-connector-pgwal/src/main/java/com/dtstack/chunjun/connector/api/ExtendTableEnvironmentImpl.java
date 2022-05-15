package com.dtstack.chunjun.connector.api;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.module.ModuleManager;

public class ExtendTableEnvironmentImpl extends TableEnvironmentImpl
        implements ExtendTableEnvironment {

    protected ExtendTableEnvironmentImpl(
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            TableConfig tableConfig,
            Executor executor,
            FunctionCatalog functionCatalog,
            ClassLoader userClassLoader) {
        super(
                catalogManager,
                moduleManager,
                tableConfig,
                executor,
                functionCatalog,
                new NoOPPlanner(),
                true,
                userClassLoader);
    }

    public static ExtendTableEnvironment create(CDCSettings settings) {
        return null;
    }
}
