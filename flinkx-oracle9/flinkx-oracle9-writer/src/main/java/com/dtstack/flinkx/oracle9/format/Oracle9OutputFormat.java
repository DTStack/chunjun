package com.dtstack.flinkx.oracle9.format;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.oracle9.Oracle9DatabaseMeta;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:29
 */
public class Oracle9OutputFormat extends JdbcOutputFormat {
    @Override
    protected Object getField(Row row, int index) {
        Object field = super.getField(row, index);
        String type = columnType.get(index);

        //oracle timestamp to oracle varchar or varchar2 or long field format
        if (!(field instanceof Timestamp)){
            return field;
        }

        if (type.equalsIgnoreCase(ColumnType.VARCHAR.name()) || type.equalsIgnoreCase(ColumnType.VARCHAR2.name())){
            SimpleDateFormat format = DateUtil.getDateTimeFormatter();
            field= format.format(field);
        }

        if (type.equalsIgnoreCase(ColumnType.LONG.name()) ){
            field = ((Timestamp) field).getTime();
        }
        return field;
    }

    @Override
    protected List<String> probeFullColumns(String table, Connection dbConn) throws SQLException {
        String schema =null;

        String[] parts = table.split("\\.");
        if(parts.length == Oracle9DatabaseMeta.DB_TABLE_PART_SIZE) {
            schema = parts[0].toUpperCase();
            table = parts[1];
        }

        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, schema, table, null);
        while(rs.next()) {
            ret.add(rs.getString("COLUMN_NAME"));
        }
        return ret;
    }

    @Override
    protected Map<String, List<String>> probePrimaryKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> map = new HashMap<>(16);

        try (PreparedStatement ps = dbConn.prepareStatement(String.format(GET_INDEX_SQL, table));
             ResultSet rs = ps.executeQuery()) {
            while(rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if(!map.containsKey(indexName)) {
                    map.put(indexName,new ArrayList<>());
                }
                map.get(indexName).add(rs.getString("COLUMN_NAME"));
            }

            Map<String,List<String>> retMap = new HashMap<>((map.size()<<2)/3);
            for(Map.Entry<String,List<String>> entry: map.entrySet()) {
                String k = entry.getKey();
                List<String> v = entry.getValue();
                if(v!=null && v.size() != 0 && v.get(0) != null) {
                    retMap.put(k, v);
                }
            }
            return retMap;
        }
    }
}
