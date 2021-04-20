package com.dtstack.flinkx;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/18
 **/
public class CastFunction extends RichMapFunction<RowData, RowData> {

    List<LogicalType> sourceTypes;
    List<LogicalType> targetTypes;
    Casting[] castings;

    public CastFunction(List<LogicalType> sourceTypes, List<LogicalType> targetTypes) {
        this.sourceTypes = sourceTypes;
        this.targetTypes = targetTypes;
        this.castings = new Casting[targetTypes.size()];
    }

    public CastFunction(LogicalType sourceTypes, LogicalType targetTypes) {
        this(
                ((RowType) sourceTypes).getChildren(),
                ((RowType) targetTypes).getChildren()
        );
    }

    public void init() throws Exception {
        if (sourceTypes.size() == targetTypes.size()) {
            for (int i = 0; i < castings.length; i++) {
                if (sourceTypes.get(i).getTypeRoot().equals(targetTypes.get(i).getTypeRoot()) ) {
                    castings[i] = (rowData, pos) -> ((GenericRowData) rowData).getField(pos);
                } else {
                    castings[i] = createCasting(sourceTypes.get(i), targetTypes.get(i));
                }
            }
        } else {
            throw new Exception("Cast Function can not generate, since sourceTypes length do not equal targetTypes");
        }
    }

    private Casting createCasting (LogicalType sourceType, LogicalType targetType) throws Exception {
        if (TypeCheckUtils.isNumeric(sourceType) &&
            TypeCheckUtils.isNumeric(targetType)) {
            return createNumericCasting(sourceType, targetType);
        } else {
            throw new Exception("Unsupported cast from to");
        }
    }

    private Casting createNumericCasting (LogicalType sourceType, LogicalType targetType) throws Exception {
        LogicalTypeRoot targetTypeRoot = targetType.getTypeRoot();
        // TODO 这个地方可能需要改成枚举， 一次匹配 Source 和 Target两种类型，inspired by flink
        if (targetTypeRoot.equals(LogicalTypeRoot.BIGINT)) {
            return sourceType.accept(new CastToBigIntLogicalTypeVisitor());
        } else {
            throw new Exception("Unsupported cast from to");
        }
    }

    @Override
    public RowData map(RowData rowData) throws Exception {
        int length = rowData.getArity();
        GenericRowData target = new GenericRowData(rowData.getArity());
        for (int i = 0; i < length; i++) {
            target.setField(i, castings[i].apply(rowData, i));
        }
        return target;
    }

}

