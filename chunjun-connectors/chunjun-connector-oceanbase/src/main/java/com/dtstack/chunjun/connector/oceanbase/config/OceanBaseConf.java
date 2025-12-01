package com.dtstack.chunjun.connector.oceanbase.config;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;

public class OceanBaseConf extends JdbcConfig {

    private String oceanBaseMode = OceanBaseMode.MYSQL.name();

    public String getOceanBaseMode() {
        return oceanBaseMode;
    }

    public void setOceanBaseMode(String oceanBaseMode) {
        this.oceanBaseMode = oceanBaseMode;
    }
}
