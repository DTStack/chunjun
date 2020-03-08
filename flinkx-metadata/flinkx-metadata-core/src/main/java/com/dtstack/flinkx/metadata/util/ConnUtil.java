package com.dtstack.flinkx.metadata.util;

import com.dtstack.flinkx.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description :
 */
public class ConnUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ConnUtil.class);

    /**
     * 关闭连接资源
     */
    public static void closeConn(ResultSet rs, Statement stmt, Connection conn, boolean commit) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LOG.warn("Close statement error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != conn) {
            try {
                if(commit){
                    commit(conn);
                }

                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * 提交事务
     */
    public static void commit(Connection conn){
        try {
            if (!conn.isClosed() && !conn.getAutoCommit()){
                conn.commit();
            }
        } catch (SQLException e){
            LOG.warn("commit error:{}", ExceptionUtil.getErrorMessage(e));
        }
    }
}
