package com.dtstack.flinkx;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/19
 **/
public class CastBigIntLogicalTypeVisitor implements LogicalTypeVisitor<Casting> {

    @Override
    public Casting visit(CharType charType) {
        return null;
    }

    @Override
    public Casting visit(VarCharType varCharType) {
        return null;
    }

    @Override
    public Casting visit(BooleanType booleanType) {
        return null;
    }

    @Override
    public Casting visit(BinaryType binaryType) {
        return null;
    }

    @Override
    public Casting visit(VarBinaryType varBinaryType) {
        return null;
    }

    @Override
    public Casting visit(DecimalType decimalType) {
        return null;
    }

    @Override
    public Casting visit(TinyIntType tinyIntType) {
        return null;
    }

    @Override
    public Casting visit(SmallIntType smallIntType) {
        return null;
    }

    @Override
    public Casting visit(IntType intType) {
        return (rowData, pos) -> new Integer(rowData.getInt(pos)).longValue();
    }

    @Override
    public Casting visit(BigIntType bigIntType) {
        return null;
    }

    @Override
    public Casting visit(FloatType floatType) {
        return null;
    }

    @Override
    public Casting visit(DoubleType doubleType) {
        return null;
    }

    @Override
    public Casting visit(DateType dateType) {
        return null;
    }

    @Override
    public Casting visit(TimeType timeType) {
        return null;
    }

    @Override
    public Casting visit(TimestampType timestampType) {
        return null;
    }

    @Override
    public Casting visit(ZonedTimestampType zonedTimestampType) {
        return null;
    }

    @Override
    public Casting visit(LocalZonedTimestampType localZonedTimestampType) {
        return null;
    }

    @Override
    public Casting visit(YearMonthIntervalType yearMonthIntervalType) {
        return null;
    }

    @Override
    public Casting visit(DayTimeIntervalType dayTimeIntervalType) {
        return null;
    }

    @Override
    public Casting visit(ArrayType arrayType) {
        return null;
    }

    @Override
    public Casting visit(MultisetType multisetType) {
        return null;
    }

    @Override
    public Casting visit(MapType mapType) {
        return null;
    }

    @Override
    public Casting visit(RowType rowType) {
        return null;
    }

    @Override
    public Casting visit(DistinctType distinctType) {
        return null;
    }

    @Override
    public Casting visit(StructuredType structuredType) {
        return null;
    }

    @Override
    public Casting visit(NullType nullType) {
        return null;
    }

    @Override
    public Casting visit(RawType<?> rawType) {
        return null;
    }

    @Override
    public Casting visit(SymbolType<?> symbolType) {
        return null;
    }

    @Override
    public Casting visit(LogicalType other) {
        return null;
    }
}
