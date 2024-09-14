package oracle2mysql.worker;

import oracle2mysql.util.DBUtils;

import java.sql.Connection;
import java.sql.ResultSet;

public class WorkerCompare  {

    public static void run(String table) {
        try (
                Connection connectionOracle = DBUtils.getOracleConnection();
                Connection connectionMysql = DBUtils.getMysqlConnection()
        ) {
            int oraclecount = -1;
            ResultSet countrsOracle = connectionOracle.prepareStatement("select count(1) from " + table).executeQuery();
            if (countrsOracle.next())
                oraclecount = countrsOracle.getInt(1);

            int mysqlcount = -1;
            ResultSet countrsMysql = connectionMysql.prepareStatement("select count(1) from " + table).executeQuery();
            if (countrsMysql.next())
                mysqlcount = countrsMysql.getInt(1);

            if (!(oraclecount == mysqlcount)) {
                System.out.println(table + "对比异常" + "  oracle行数:" + oraclecount + "  mysql行数：" + mysqlcount);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
