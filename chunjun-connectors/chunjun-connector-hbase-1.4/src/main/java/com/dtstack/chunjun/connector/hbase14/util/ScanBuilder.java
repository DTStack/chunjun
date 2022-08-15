package com.dtstack.chunjun.connector.hbase14.util;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;

import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ScanBuilder implements Serializable {

    private static final long serialVersionUID = 12L;

    private final boolean isSync;
    private final HBaseTableSchema hBaseTableSchema;
    private final List<FieldConf> fieldConfList;

    private ScanBuilder(HBaseTableSchema hBaseTableSchema) {
        this.isSync = false;
        this.fieldConfList = null;
        this.hBaseTableSchema = hBaseTableSchema;
    }

    private ScanBuilder(List<FieldConf> fieldConfList) {
        this.isSync = true;
        this.fieldConfList = fieldConfList;
        this.hBaseTableSchema = null;
    }

    public static ScanBuilder forSql(HBaseTableSchema hBaseTableSchema) {
        return new ScanBuilder(hBaseTableSchema);
    }

    public static ScanBuilder forSync(List<FieldConf> fieldConfList) {
        return new ScanBuilder(fieldConfList);
    }

    public Scan buildScan() {
        Scan scan = new Scan();
        if (isSync) {
            for (FieldConf fieldConf : fieldConfList) {
                String fieldName = fieldConf.getName();
                if (!"rowkey".equalsIgnoreCase(fieldName)) {
                    if (fieldName.contains(".")) {
                        String[] fields = fieldName.split("\\.");
                        scan.addColumn(Bytes.toBytes(fields[0]), Bytes.toBytes(fields[1]));
                    }
                }
            }
            return scan;
        } else {
            String[] familyNames = this.hBaseTableSchema.getFamilyNames();
            for (String familyName : familyNames) {
                Map<String, DataType> familyInfo = hBaseTableSchema.getFamilyInfo(familyName);
                for (Map.Entry<String, DataType> columnInfoEntry : familyInfo.entrySet()) {
                    scan.addColumn(
                            Bytes.toBytes(familyName), Bytes.toBytes(columnInfoEntry.getKey()));
                }
            }
        }
        return scan;
    }
}
