CREATE TABLE IF NOT EXISTS chunjun_dirty_data
(
    job_id        VARCHAR(32)                               NOT NULL COMMENT 'Flink Job Id',
    job_name      VARCHAR(255)                              NOT NULL COMMENT 'Flink Job Name',
    operator_name VARCHAR(255)                              NOT NULL COMMENT '出现异常数据的算子名，包含表名',
    dirty_data    TEXT                                      NOT NULL COMMENT '脏数据的异常数据',
    error_message TEXT COMMENT '脏数据中异常原因',
    field_name    VARCHAR(255) COMMENT '脏数据中异常字段名',
    create_time   TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '脏数据出现的时间点'
    )
    COMMENT '存储脏数据';

CREATE INDEX idx_job_id ON chunjun_dirty_data (job_id);
CREATE INDEX idx_operator_name ON chunjun_dirty_data (operator_name);
CREATE INDEX idx_create_time ON chunjun_dirty_data (create_time);
