package com.dtstack.flinkx.cdc;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
/**
 * @author tiezhu@dtstack.com
 * @since 19/11/2021 Friday
 *     <p>Flinkx DDL row data
 */
public class DdlRowData implements RowData, Serializable {

    /** ddl sql content */
    private String sql;

    /** ddl table-name with schema-name */
    private String tableIdentifier;

    /** binlog event type */
    private EventType type;

    private RowKind rowKind;

    private String lsn;

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    @Override
    public boolean isNullAt(int i) {
        return false;
    }

    @Override
    public boolean getBoolean(int i) {
        return false;
    }

    @Override
    public byte getByte(int i) {
        return 0;
    }

    @Override
    public short getShort(int i) {
        return 0;
    }

    @Override
    public int getInt(int i) {
        return 0;
    }

    @Override
    public long getLong(int i) {
        return 0;
    }

    @Override
    public float getFloat(int i) {
        return 0;
    }

    @Override
    public double getDouble(int i) {
        return 0;
    }

    @Override
    public StringData getString(int i) {
        return null;
    }

    @Override
    public DecimalData getDecimal(int i, int i1, int i2) {
        return null;
    }

    @Override
    public TimestampData getTimestamp(int i, int i1) {
        return null;
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];
    }

    @Override
    public ArrayData getArray(int i) {
        return null;
    }

    @Override
    public MapData getMap(int i) {
        return null;
    }

    @Override
    public RowData getRow(int i, int i1) {
        return null;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public void setTableIdentifier(String tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public void setType(String type) {
        this.type = EventType.valueOf(type);
    }

    public String getLsn() {
        return lsn;
    }

    public void setLsn(String lsn) {
        this.lsn = lsn;
    }
}
