package com.dtstack.metadata.rdb.core.entity;

import java.io.Serializable;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-20 14:28
 * @Description:
 */
public class ConnectionInfo implements Serializable {

    private String jdbcUrl;

    private String username;

    private String password;

    private String driver;

    private int timeout ;

    public ConnectionInfo() {
    }

    public ConnectionInfo(ConnectionInfo connectionInfo) {
        this.jdbcUrl = connectionInfo.jdbcUrl;
        this.password = connectionInfo.password;
        this.username = connectionInfo.username;
        this.driver = connectionInfo.driver;
        this.timeout = connectionInfo.getTimeout();
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
