package oracle2mysql.worker;

import oracle2mysql.util.ConfigUtil;
import oracle2mysql.util.DBUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class WorkerOracle2Txt {

    final static String fieldEncloseStr = "\0";
    final static String fieldSepStr = "|";
    final static String rowSepStr = "\n";
    final static String nullStr = "\\N";

    public static void run(String table)  {
        String sql = "select * from " + table;
        File file = new File(ConfigUtil.getConfig("dataDir") + table + ".txt");
        if (file.exists())
            file.delete();
        try (
                Connection connection = DBUtils.getOracleConnection();
                PreparedStatement ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = ps.executeQuery();
                Writer fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
        ) {

            //设置流式获取数据 避免OOM
            ps.setFetchSize(Integer.valueOf(ConfigUtil.getConfig("fetchSize")));

            ResultSetMetaData md = rs.getMetaData();

            while (rs.next()) {
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    //fw.write("\0");
                    fw.write(fieldEncloseStr);
                    String tmp = "";
                    if (!md.getColumnTypeName(i).contains("BLOB")) {
                        tmp = rs.getString(i);
                        if (tmp == null)
                            tmp = "";

                        //处理转义符和字段终止符
                        if (!tmp.equals("")) {
                            tmp = tmp.replace(fieldEncloseStr, "").replace("\\", "\\\\");
                        } else {
                            //空值填成 \N 防止导入mysql 默认值
                            tmp = nullStr;
                        }
                    } else {
                        //不支持BLOB
                        tmp = nullStr;
                    }


                    fw.write(tmp);
                    fw.write(fieldEncloseStr);
                    if (i != md.getColumnCount()) {
                        fw.write(fieldSepStr);
                    } else {
                        fw.write(rowSepStr);
                    }
                }
            }

            fw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
