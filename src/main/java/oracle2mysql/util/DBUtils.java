package oracle2mysql.util;


import java.sql.Connection;
import java.sql.DriverManager;

public class DBUtils {



    private static String oracleUrl = ConfigUtil.getConfig("oracle.url");
    private static String oracleUser = ConfigUtil.getConfig("oracle.user");
    private static String oraclePassword = ConfigUtil.getConfig("oracle.password");

    private static String mysqlUrl = ConfigUtil.getConfig("mysql.url");
    private static String mysqlUser = ConfigUtil.getConfig("mysql.user");
    private static String mysqlPassword = ConfigUtil.getConfig("mysql.password");

    public static Connection getOracleConnection() throws Exception {
        return DriverManager.getConnection(oracleUrl, oracleUser, oraclePassword);
    }

    public static Connection getMysqlConnection() throws Exception {
        return DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
    }
}
