package com.dtstack.flinkx.cdc;

import com.dtstack.flinkx.element.AbstractBaseColumn;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tiezhu@dtstack.com
 * @since 19/11/2021 Friday
 *     <p>Flinkx DDL row data
 */
public class DdlRowData implements RowData, Serializable {

    private RowKind rowKind;

    private final String[] headers;

    private final AbstractBaseColumn[] columns;

    public DdlRowData(String[] headers) {
        this.headers = headers;
        this.columns = new AbstractBaseColumn[headers.length];
    }

    public void setColumn(int index, AbstractBaseColumn column) {
        columns[index] = column;
    }

    public AbstractBaseColumn getField(int pos) {
        return columns[pos];
    }

    @Override
    public int getArity() {
        return columns.length;
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
    public boolean isNullAt(int pos) {
        return this.columns[pos] == null || this.columns[pos].getData() == null;
    }

    @Override
    public boolean getBoolean(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to boolean");
    }

    @Override
    public byte getByte(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to byte");
    }

    @Override
    public short getShort(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to short");
    }

    @Override
    public int getInt(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to int");
    }

    @Override
    public long getLong(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to long");
    }

    @Override
    public float getFloat(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to float");
    }

    @Override
    public double getDouble(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to double");
    }

    @Override
    public StringData getString(int i) {
        return new BinaryStringData(columns[i].asString());
    }

    @Override
    public DecimalData getDecimal(int i, int i1, int i2) {
        throw new UnsupportedOperationException("DDL RowData can't transform to decimal");
    }

    @Override
    public TimestampData getTimestamp(int i, int i1) {
        throw new UnsupportedOperationException("DDL RowData can't transform to timestamp");
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to RawValueData");
    }

    @Override
    public byte[] getBinary(int i) {
        return columns[i].asBinary();
    }

    @Override
    public ArrayData getArray(int i) {
        return new GenericArrayData(columns);
    }

    @Override
    public MapData getMap(int i) {
        final Map<String, Object> ddlMap = new HashMap<>();
        for (int j = 0; j < columns.length; j++) {
            ddlMap.put(headers[j], columns[j]);
        }
        return new GenericMapData(ddlMap);
    }

    @Override
    public RowData getRow(int i, int i1) {
        throw new UnsupportedOperationException("DDL RowData can't transform to Row");
    }

    public String getSql() {
        for (int i = 0; i < headers.length; i++) {
            if ("content".equalsIgnoreCase(headers[i])) {
                return columns[i].asString();
            }
        }
        throw new IllegalArgumentException("Can not find content from DDL RowData!");
    }

    public String getTableIdentifier() {
        for (int i = 0; i < headers.length; i++) {
            if ("tableIdentifier".equalsIgnoreCase(headers[i])) {
                return columns[i].asString();
            }
        }
        throw new IllegalArgumentException("Can not find tableIdentifier from DDL RowData!");
    }

    public EventType getType() {
        for (int i = 0; i < headers.length; i++) {
            if ("type".equalsIgnoreCase(headers[i])) {
                return EventType.valueOf(columns[i].asString());
            }
        }
        throw new IllegalArgumentException("Can not find type from DDL RowData!");
    }

    public String getLsn() {
        for (int i = 0; i < headers.length; i++) {
            if ("lsn".equalsIgnoreCase(headers[i])) {
                return columns[i].asString();
            }
        }
        throw new IllegalArgumentException("Can not find lsn from DDL RowData!");
    }

    public String[] getHeaders() {
        return headers;
    }
}
