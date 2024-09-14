package oracle2mysql.worker;

import oracle2mysql.util.ConfigUtil;
import oracle2mysql.util.DBUtils;

import java.sql.Connection;
import java.util.Properties;

public class WorkerTxt2Mysql {

    final static String fieldEncloseStr = "\0";
    final static String fieldSepStr = "|";
    final static String rowSepStr = "\n";
    final static String nullStr = "\\N";

    public static void run(String table) {
        try (
                Connection connection = DBUtils.getMysqlConnection();
        ) {
            String sql = String.format(
                    "LOAD DATA LOCAL INFILE '%s/%s.txt' " +
                            "INTO TABLE" +
                            " %s " +
                            "CHARACTER SET utf8mb4 " +
                            "FIELDS TERMINATED BY '%s'  " +
                            "ENCLOSED BY '%s'  " +
                            "LINES TERMINATED BY '%s'",
                    ConfigUtil.getConfig("dataDir"), table, table, fieldSepStr, fieldEncloseStr, rowSepStr);
            //清空表
            connection.prepareStatement("truncate table " + table).execute();
            //导入数据
            connection.prepareStatement(sql).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
