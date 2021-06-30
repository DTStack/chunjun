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

CREATE TABLE kafka_source
(
    val_bool   boolean,
    val_int    int,
    val_long   bigint,
    val_str    string,
    val_float  float,
    val_double double,
    val_date   timestamp
) WITH (
    'connector' = 'kafka-x',
    'topic' = 'luna',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'luna_g',
    'format' = 'json',
    'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE solr_sink
(
    val_bool   boolean,
    val_int    int,
    val_long   bigint,
    val_str    string,
    val_float  float,
    val_double double,
    val_date   timestamp
) WITH (
    'connector' = 'solr-x',
    'zk-hosts' = 'master:2181,worker:2181,tools:2181',
    'zk-chroot' = '/solr',
    'collection' = 'flink_dev',
    'krb5conf' = './krb5.conf',
    'keytab' = './solr.keytab',
    'principal' = 'solr/worker@DTSTACK.COM',
    'sink.parallelism' = '1',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '5000'
);

INSERT INTO solr_sink
SELECT *
FROM kafka_source;
