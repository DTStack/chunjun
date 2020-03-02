package bigdata;

import com.dtstack.flinkx.mysql.MySqlDatabaseMeta;
import com.dtstack.flinkx.rdb.util.DbUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;


public class TestMysqlPk {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        MySqlDatabaseMeta databaseMeta = new MySqlDatabaseMeta();
        Class.forName(databaseMeta.getDriverClass());
        Connection conn = DriverManager.getConnection("jdbc:mysql://172.16.8.104:3306/test?useCursorFetch=true", "dtstack", "abc123");
        //List<String> list = databaseMeta.listUniqueKeys("sb250", conn);
        //System.out.println(list);
        Map map = DbUtil.getPrimaryOrUniqueKeys("sb252", conn);
        System.out.println(map);
    }
}
