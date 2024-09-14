package oracle2mysql.worker;

import oracle2mysql.util.DBUtils;

import java.sql.Connection;

public class WorkerMysql {




    public static void run(String table) {
        try (
                Connection connectionMysql = DBUtils.getMysqlConnection();
        ) {
            String sql = table;
            connectionMysql.prepareStatement(sql).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
