package com.dtstack.chunjun.connector.oceanbase.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.oracle.converter.BlobType;
import com.dtstack.chunjun.connector.oracle.converter.ClobType;
import com.dtstack.chunjun.connector.oracle.converter.ConvertUtil;
import com.dtstack.chunjun.connector.oracle.converter.OracleSyncConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.element.column.ZonedTimestampColumn;

import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import com.oceanbase.jdbc.Blob;
import com.oceanbase.jdbc.Clob;
import com.oceanbase.jdbc.extend.datatype.DataTypeUtilities;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;

import java.sql.Timestamp;
import java.util.TimeZone;

public class OceanbaseOracleSyncConverter extends OracleSyncConverter {

    public OceanbaseOracleSyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case VARCHAR:
                if (type instanceof ClobType) {
                    return val -> {
                        Clob clob = (Clob) val;
                        return new StringColumn(ConvertUtil.convertClob(clob));
                    };
                }
                return val -> new StringColumn(val.toString());
            case VARBINARY:
                return val -> {
                    if (type instanceof BlobType) {
                        Blob blob = (Blob) val;
                        byte[] bytes = blob.getBytes(1, (int) blob.length());
                        return new BytesColumn(bytes);
                    } else {
                        return new BytesColumn((byte[]) val);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int precision = ((TimestampType) type).getPrecision();
                if (precision == 6) {
                    return val -> new TimestampColumn((Timestamp) val, 0); // java.sql.Timestamp
                }
            case TIMESTAMP_WITH_TIME_ZONE:
                if (type instanceof ZonedTimestampType) {
                    final int zonedPrecision = ((ZonedTimestampType) type).getPrecision();
                    return val -> {
                        TIMESTAMPTZ timestamptz = (TIMESTAMPTZ) val;
                        Timestamp timestamp = timestamptz.timestampValue();
                        return new ZonedTimestampColumn(timestamp, zonedPrecision);
                    };
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (type instanceof LocalZonedTimestampType) {
                    final int localPrecision = ((LocalZonedTimestampType) type).getPrecision();
                    return val -> {
                        TIMESTAMPLTZ timestamptz = (TIMESTAMPLTZ) val;
                        // 重写处理12个字节情况，TIMESTAMPLTZ#toTimestamp
                        byte[] bytes = timestamptz.toBytes(); // 获取字节码
                        TimeZone timeZone;
                        if (bytes.length >= 14) {
                            // 字节数组长度足够，尝试提取时区信息
                            String tzStr =
                                    DataTypeUtilities.toTimezoneStr(
                                            bytes[12], bytes[13], "GMT", true);
                            timeZone = TimeZone.getTimeZone(tzStr);
                        } else {
                            // 字节数组长度不足，使用默认时区
                            timeZone = TimeZone.getDefault();
                        }
                        Timestamp timestamp =
                                new Timestamp(DataTypeUtilities.getOriginTime(bytes, timeZone));
                        timestamp.setNanos(DataTypeUtilities.getNanos(bytes, 7));
                        return new ZonedTimestampColumn(timestamp, timeZone, localPrecision);
                    };
                }
        }
        return super.createInternalConverter(type);
    }
}
