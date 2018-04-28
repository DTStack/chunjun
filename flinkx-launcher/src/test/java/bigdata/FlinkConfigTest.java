package bigdata;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;

import java.io.File;

/**
 * Created by softfly on 18/4/24.
 */
public class FlinkConfigTest {
    public static void main(String[] args) {
        //Configuration config = GlobalConfiguration.loadConfiguration("/hadoop/flink-1.4.0/conf");
        //System.out.println(config.getString(JobManagerOptions.ADDRESS));
        String msg = "xxx" + File.separator;
        System.out.println(msg);
    }
}
