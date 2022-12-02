/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.nebula.client;

import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.utils.GraphUtil;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.CREATE_EDGE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.CREATE_VERTEX;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.NEBULA_PROP;

public class NebulaSession implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(NebulaSession.class);

    private final NebulaConfig nebulaConfig;

    private NebulaPool pool;

    private Session session;

    public NebulaSession(NebulaConfig nebulaConfig) {
        this.nebulaConfig = nebulaConfig;
    }

    /**
     * init nebula pool and session
     *
     * @return
     * @throws Exception
     */
    public void init() throws Exception {
        pool = new NebulaPool();
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setTimeout(nebulaConfig.getTimeout());
        nebulaPoolConfig.setIntervalIdle(nebulaConfig.getIntervalIdle());
        nebulaPoolConfig.setMaxConnSize(nebulaConfig.getMaxConnsSize());
        nebulaPoolConfig.setMinConnSize(nebulaConfig.getMinConnsSize());
        nebulaPoolConfig.setWaitTime(nebulaConfig.getWaitTime());
        nebulaPoolConfig.setIdleTime(nebulaConfig.getIdleTime());
        nebulaPoolConfig.setEnableSsl(nebulaConfig.getEnableSSL());
        nebulaPoolConfig.setSslParam(GraphUtil.getSslParam(nebulaConfig));

        boolean init = pool.init(nebulaConfig.getGraphdAddresses(), nebulaPoolConfig);
        if (!init) {
            throw new Exception("inited nebula pool failed");
        }
        session =
                pool.getSession(
                        nebulaConfig.getUsername(),
                        nebulaConfig.getPassword(),
                        nebulaConfig.getReconn());
        if (!session.ping()) {
            throw new Exception("inited nebula session failed");
        }
    }

    /**
     * execute ngql the parameters
     *
     * @param ngql
     * @param params
     * @return
     * @throws IOErrorException
     */
    public ResultSet executeWithParameter(String ngql, Map<String, Object> params)
            throws IOErrorException {
        return session.executeWithParameter(ngql, params);
    }

    public ResultSet execute(String ngql) throws IOErrorException {
        return session.execute(ngql);
    }

    public Session getSession() throws Exception {
        if (!session.ping()) {
            session =
                    pool.getSession(
                            nebulaConfig.getUsername(),
                            nebulaConfig.getPassword(),
                            nebulaConfig.getReconn());
        }
        return session;
    }

    /** close the pool and release session */
    public void close() {
        if (session != null) {
            session.release();
        }

        if (pool != null) {
            pool.close();
        }
    }

    public ResultSet createSpace(String space) throws IOErrorException {
        String statement =
                String.format(
                        "CREATE SPACE IF NOT EXISTS `%s` (vid_type=%s);",
                        space, nebulaConfig.getVidType());
        LOG.debug("ngql is : {}", statement);
        ResultSet resultSet = session.execute(statement);
        return resultSet;
    }

    public ResultSet createSchema(String space, String schemaName) throws IOErrorException {
        String statement = null;
        String prop =
                String.join(
                        ",",
                        nebulaConfig.getFields().stream()
                                .map(
                                        fieldConf ->
                                                String.format(
                                                        NEBULA_PROP,
                                                        fieldConf.getName(),
                                                        convertToNebulaType(fieldConf.getType())))
                                .collect(Collectors.toList()));
        switch (nebulaConfig.getSchemaType()) {
            case TAG:
            case VERTEX:
                statement = String.format(CREATE_VERTEX, schemaName, prop);
                break;
            case EDGE:
            case EDGE_TYPE:
                statement = String.format(CREATE_EDGE, schemaName, prop);
                break;
            default:
                throw new UnsupportedTypeException(
                        "unsupport schema type: " + nebulaConfig.getSchemaType());
        }
        LOG.debug("ngql is : {}", statement);
        ResultSet resultSet = session.execute(String.format("use %s;%s;", space, statement));
        return resultSet;
    }

    private String convertToNebulaType(String type) {
        String var = type.toLowerCase(Locale.ROOT).split("\\(")[0];
        switch (var) {
            case "bigint":
                return "INT64";
            case "boolean":
                return "bool";
            case "tinyint":
                return "INT8";
            case "smallint":
                return "INT16";
            case "integer":
            case "int":
                return "INT32";
            case "float":
                return "FLOAT";
            case "double":
                return "DOUBLE";
            case "varchar":
                return "string";
            case "date":
                return "date";
            case "timestamp":
                return "timestamp";
            case "time":
                return "time";
            case "timestamp_with_time_zone":
            case "timestamp_without_time_zone":
            case "timestamp_with_local_time_zone":
                return "datetime";
            case "string":
                return String.format("FIXED_STRING(%d)", nebulaConfig.getStringLength());
            default:
                throw new UnsupportedTypeException(
                        "nebula do not support data type: " + type.toLowerCase(Locale.ROOT));
        }
    }
}
