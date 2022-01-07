package com.dtstack.flinkx.connector.kafka.serialization.ticdc;

import com.dtstack.flinkx.cdc.EventType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/20 星期一
 */
public class TicdcEventTypeHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TicdcEventTypeHelper.class);

    private TicdcEventTypeHelper() {}

    /**
     * Transforms ddl_type index into DDLType.
     *
     * @param type ddl_type index.
     * @return ddlType
     */
    public static EventType findDDL(int type) {
        switch (type) {
            case 1:
                return EventType.CREATE_SCHEMA;
            case 2:
                return EventType.DROP_SCHEMA;
            case 3:
                return EventType.CREATE_TABLE;
            case 4:
                return EventType.DROP_TABLE;
            case 5:
                return EventType.ADD_COLUMN;
            case 6:
                return EventType.DROP_COLUMN;
            case 7:
                return EventType.ADD_INDEX;
            case 8:
                return EventType.DROP_INDEX;
            case 9:
                return EventType.ADD_FOREIGN_KEY;
            case 10:
                return EventType.DROP_FOREIGN_KEY;
            case 11:
                return EventType.TRUNCATE_TABLE;
            case 12:
                return EventType.MODIFY_COLUMN;
            case 13:
                return EventType.REBASE_AUTO_ID;
            case 14:
                return EventType.RENAME_TABLE;
            case 15:
                return EventType.SET_DEFAULT_VALUE;
            case 16:
                return EventType.SHARD_ROW_ID;
            case 17:
                return EventType.MODIFY_TABLE_COMMENT;
            case 18:
                return EventType.RENAME_INDEX;
            case 19:
                return EventType.ADD_TABLE_PARTITION;
            case 20:
                return EventType.DROP_TABLE_PARTITION;
            case 21:
                return EventType.CREATE_VIEW;
            case 22:
                return EventType.MODIFY_TABLE_CHARSET_AND_COLLATE;
            case 23:
                return EventType.TRUNCATE_TABLE_PARTITION;
            case 24:
                return EventType.DROP_VIEW;
            case 25:
                return EventType.RECOVER_TABLE;
            case 26:
                return EventType.MODIFY_SCHEMA_CHARSET_AND_COLLATE;
            case 27:
                return EventType.LOCK_TABLE;
            case 28:
                return EventType.UNLOCK_TABLE;
            case 29:
                return EventType.REPAIR_TABLE;
            case 30:
                return EventType.SET_TIFLASH_REPLICA;
            case 31:
                return EventType.UPDATE_TIFLASH_REPLICA_STATUS;
            case 32:
                return EventType.ADD_PRIMARY_KEY;
            case 33:
                return EventType.DROP_PRIMARY_KEY;
            case 34:
                return EventType.CREATE_SEQUENCE;
            case 35:
                return EventType.ALTER_SEQUENCE;
            case 36:
                return EventType.DROP_SEQUENCE;
            case -1:
            default:
                LOG.warn(
                        "Can not transform the ddl type_index: "
                                + type
                                + ", use [UNKNOWN] instead.");
                return EventType.UNKNOWN;
        }
    }

    /**
     * Transforms dml_type index into DMLType.
     *
     * @param type dml_type index.
     * @return dmlType
     */
    public static EventType findDML(int type) {
        switch (type) {
            case 1:
                return EventType.UPDATE;
            case 2:
                return EventType.DELETE;
            case 3:
                return EventType.INSERT;
            case -1:
            default:
                LOG.warn(
                        "Can not transform the dml type_index: "
                                + type
                                + ", use [UNKNOWN] instead.");
                return EventType.UNKNOWN;
        }
    }
}
