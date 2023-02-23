package com.dtstack.chunjun.connector.selectdbcloud.sink;

import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConf;

import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

public class CopySQLBuilder {
    private static final String COPY_SYNC = "copy.async";
    private static final String COPY_DELETE = "copy.use_delete_sign";
    private final SelectdbcloudConf conf;
    private final String fileName;
    private final Properties properties;

    public CopySQLBuilder(SelectdbcloudConf conf, String fileName) {
        this.conf = conf;
        this.fileName = fileName;
        this.properties = conf.getLoadProperties();
    }

    public String buildCopySQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ")
                .append(conf.getTableIdentifier())
                .append(" FROM @~('")
                .append(fileName)
                .append("') ")
                .append("PROPERTIES (");

        // copy into must be sync
        properties.put(COPY_SYNC, false);
        if (conf.getEnableDelete()) {
            properties.put(COPY_DELETE, true);
        }
        StringJoiner props = new StringJoiner(",");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            String prop = String.format("'%s'='%s'", key, value);
            props.add(prop);
        }
        sb.append(props).append(" )");
        return sb.toString();
    }
}
