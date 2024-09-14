package oracle2mysql.worker;

import oracle2mysql.util.DBUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class WorkerFixAutoIncrement {

    //尝试修复自增主键
    public static void run(String table) {
        //由于很难判断oracle是否自增主键，当前仅选取列名是id，类型为数字且为主键的表
        try (Connection oracleCon = DBUtils.getOracleConnection();
             Connection mysqlCon = DBUtils.getMysqlConnection()) {

            ResultSet rs = oracleCon.prepareStatement("" +
                    "SELECT t.INDEX_NAME,\n" +
                    "       t.TABLE_NAME,\n" +
                    "       t.COLUMN_NAME,\n" +
                    "       t.COLUMN_POSITION,\n" +
                    "       t.COLUMN_LENGTH,\n" +
                    "       t.CHAR_LENGTH,\n" +
                    "       t.DESCEND\n" +
                    "  FROM USER_IND_COLUMNS T,\n" +
                    "       USER_INDEXES     I,\n" +
                    "       USER_CONSTRAINTS c,\n" +
                    "       USER_TAB_COLUMNS U\n" +
                    " WHERE T.INDEX_NAME = I.INDEX_NAME\n" +
                    "   AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)\n" +
                    "   AND T.TABLE_NAME = U.TABLE_NAME\n" +
                    "   AND T.COLUMN_NAME = U.COLUMN_NAME\n" +
                    "   and i.index_type != 'FUNCTION-BASED NORMAL'\n" +
                    "   and C.CONSTRAINT_TYPE = 'P'\n" +
                    "   and t.COLUMN_NAME = 'ID'\n" +
                    "   AND U.DATA_TYPE = 'NUMBER'\n" +
                    "   and t.TABLE_NAME = '" + table + "'").executeQuery();

            if (rs.next()) {
                ResultSet maxIdRs = mysqlCon.prepareStatement("select max(id) from  " + table).executeQuery();

                int autoIncrement;

                maxIdRs.next();
                autoIncrement = maxIdRs.getInt(1) + 1;

                mysqlCon.prepareStatement("alter table " + table + " auto_increment = " + autoIncrement).execute();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
