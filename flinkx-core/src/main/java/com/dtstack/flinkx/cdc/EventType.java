package com.dtstack.flinkx.cdc;

/**
 * @author tiezhu@dtstack.com
 * @since 19/11/2021 Friday
 */
public enum EventType {
    UNKNOWN("UNKNOWN", -1),

    CREATE_SCHEMA("CREATE_SCHEMA", 1),

    DROP_SCHEMA("DROP_SCHEMA", 2),

    CREATE_TABLE("CREATE_TABLE", 3),

    DROP_TABLE("DROP_TABLE", 4),

    ADD_COLUMN("ADD_COLUMN", 5),

    DROP_COLUMN("DROP_COLUMN", 6),

    ADD_INDEX("ADD_INDEX", 7),

    DROP_INDEX("DROP_INDEX", 8),

    ADD_FOREIGN_KEY("ADD_FOREIGN_KEY", 9),

    DROP_FOREIGN_KEY("DROP_FOREIGN_KEY", 10),

    TRUNCATE_TABLE("TRUNCATE_TABLE", 11),

    MODIFY_COLUMN("MODIFY_COLUMN", 12),

    REBASE_AUTO_ID("REBASE_AUTO_ID", 13),

    RENAME_TABLE("RENAME_TABLE", 14),

    SET_DEFAULT_VALUE("SET_DEFAULT_VALUE", 15),

    SHARD_ROW_ID("SHARD_ROW_ID", 16),

    MODIFY_TABLE_COMMENT("MODIFY_TABLE_COMMENT", 17),

    RENAME_INDEX("RENAME_INDEX", 18),

    ADD_TABLE_PARTITION("ADD_TABLE_PARTITION", 19),

    DROP_TABLE_PARTITION("DROP_TABLE_PARTITION", 20),

    CREATE_VIEW("CREATE_VIEW", 21),

    MODIFY_TABLE_CHARSET_AND_COLLATE("MODIFY_TABLE_CHARSET_AND_COLLATE", 22),

    TRUNCATE_TABLE_PARTITION("TRUNCATE_TABLE_PARTITION", 23),

    DROP_VIEW("DROP_VIEW", 24),

    RECOVER_TABLE("RECOVER_TABLE", 25),

    MODIFY_SCHEMA_CHARSET_AND_COLLATE("MODIFY_SCHEMA_CHARSET_AND_COLLATE", 26),

    LOCK_TABLE("LOCK_TABLE", 27),

    UNLOCK_TABLE("UNLOCK_TABLE", 28),

    REPAIR_TABLE("REPAIR_TABLE", 29),

    SET_TIFLASH_REPLICA("SET_TIFLASH_REPLICA", 30),

    UPDATE_TIFLASH_REPLICA_STATUS("UPDATE_TIFLASH_REPLICA_STATUS", 31),

    ADD_PRIMARY_KEY("ADD_PRIMARY_KEY", 32),

    DROP_PRIMARY_KEY("DROP_PRIMARY_KEY", 33),

    CREATE_SEQUENCE("CREATE_SEQUENCE", 34),

    ALTER_SEQUENCE("ALTER_SEQUENCE", 35),

    DROP_SEQUENCE("DROP_SEQUENCE", 36),

    ALTER_TABLE("ALTER_TABLE", 37),

    CREATE_INDEX("CREATE_INDEX", 38),

    CREATE_USER("CREATE_USER", 39),

    DROP_USER("DROP_USER", 40),

    // DML
    INSERT("INSERT", 101),

    DELETE("DELETE", 102),

    UPDATE("UPDATE", 103),

    QUERY("QUERY", 104),

    // DDL simple name
    ALTER("ALTER", 105),

    TRUNCATE("TRUNCATE", 106),

    DROP("DROP", 107),

    RENAME("RENAME", 108),
    ;

    private final String value;
    private final int index;

    EventType(String type, int index) {
        this.value = type;
        this.index = index;
    }

    public static EventType valueOf(int type) {
        EventType[] eventTypes = values();
        for (EventType eventType : eventTypes) {
            if (eventType.getIndex() == type) {
                return eventType;
            }
        }
        return UNKNOWN;
    }

    public static EventType valuesOf(String value) {
        EventType[] eventTypes = values();
        for (EventType eventType : eventTypes) {
            if (eventType.value.equalsIgnoreCase(value)) {
                return eventType;
            }
        }
        return UNKNOWN;
    }

    public String getValue() {
        return value;
    }

    public int getIndex() {
        return index;
    }

    public boolean isDelete() {
        return this.equals(DELETE);
    }

    public boolean isUpdate() {
        return this.equals(UPDATE);
    }

    public boolean isInsert() {
        return this.equals(INSERT);
    }

    public boolean isQuery() {
        return this.equals(QUERY);
    }

    public boolean isDML() {
        return isDelete() || isInsert() || isUpdate() || isQuery();
    }

    public boolean isDDL() {
        return !isDML();
    }
}
