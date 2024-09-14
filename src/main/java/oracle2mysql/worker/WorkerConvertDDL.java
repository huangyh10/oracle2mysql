package oracle2mysql.worker;

import cn.hutool.core.util.StrUtil;
import oracle2mysql.util.DBUtils;

import java.sql.*;
import java.util.*;

public class WorkerConvertDDL {


    public static void run(String table) {
        try (Connection mysqlConn = DBUtils.getMysqlConnection()) {

            try {
                String dropSql = "drop table if exists " + table;
                mysqlConn.prepareStatement(dropSql).execute();

                // 执行建表语句
                String createTableSql = genCol(table, "N");
                mysqlConn.prepareStatement(createTableSql).execute();

                // 执行建索引语句
                String[] createIndexSql = genIndex(table);
                if (createIndexSql.length > 0) {
                    for (String sql : createIndexSql) {
                        if (!"".equals(sql))
                        mysqlConn.prepareStatement(sql).execute();
                    }
                }
            } catch (SQLSyntaxErrorException e) {
                //报错后 进入修复模式
                String dropSql = "drop table if exists " + table;
                mysqlConn.prepareStatement(dropSql).execute();

                // 执行建表语句
                String createTableSql = genCol(table, "Y");
                mysqlConn.prepareStatement(createTableSql).execute();

                // 执行建索引语句
                String[] createIndexSql = genIndex(table);
                if (createIndexSql.length > 0) {
                    for (String sql : createIndexSql) {
                        if (!"".equals(sql))
                        mysqlConn.prepareStatement(sql).execute();
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("报错表: " + table);
            System.out.println("报错语句: \n" + genCol(table, "Y"));
            for (String sql : genIndex(table)){
                System.out.println(sql);
            }
            System.out.println("报错原因: " + e.getMessage());
            e.printStackTrace();
        }
    }


    static String genCol(String oracleTablename, String fixMode) {
        ArrayList<HashMap> result = new ArrayList<>();

        try (Connection oracleConn = DBUtils.getOracleConnection()) {
            String sql = String.format(
                    "SELECT A.COLUMN_NAME,\n" +
                            "       A.DATA_TYPE,\n" +
                            "       A.CHAR_LENGTH,\n" +
                            "       case\n" +
                            "         when A.DATA_PRECISION is null then\n" +
                            "          -1\n" +
                            "         else\n" +
                            "          A.DATA_PRECISION\n" +
                            "       end DATA_PRECISION,\n" +
                            "       case\n" +
                            "         when A.DATA_SCALE is null then\n" +
                            "          -1\n" +
                            "         when A.DATA_SCALE > 30 then\n" +
                            "          least(A.DATA_PRECISION, 30) - 1\n" +
                            "         else\n" +
                            "          A.DATA_SCALE\n" +
                            "       end DATA_SCALE,\n" +
                            "       case\n" +
                            "         when A.NULLABLE = 'Y' THEN\n" +
                            "          'True'\n" +
                            "         ELSE\n" +
                            "          'False'\n" +
                            "       END as isnull,\n" +
                            "       B.COMMENTS,\n" +
                            "       A.DATA_DEFAULT,\n" +
                            "       case\n" +
                            "         when a.AVG_COL_LEN is null then\n" +
                            "          -1\n" +
                            "         else\n" +
                            "          a.AVG_COL_LEN\n" +
                            "       end AVG_COL_LEN\n" +
                            "  FROM USER_TAB_COLUMNS A\n" +
                            "  LEFT JOIN USER_COL_COMMENTS B\n" +
                            "    ON A.TABLE_NAME = B.TABLE_NAME\n" +
                            "   AND A.COLUMN_NAME = B.COLUMN_NAME\n" +
                            " WHERE A.TABLE_NAME = upper('%s')\n" +
                            " ORDER BY COLUMN_ID ASC\n", oracleTablename);
            ResultSet rs = oracleConn.prepareStatement(sql).executeQuery();
            while (rs.next()) {

                String fieldName = rs.getString(1);
                String fieldType = rs.getString(2);
                int fieldLength = rs.getInt(3);
                int fieldDataPricision = rs.getInt(4);
                int fieldDataScale = rs.getInt(5);
                String fieldIsNull = rs.getString(6);
                String fieldComment = rs.getString(7);
                String fieldDefaultStr = rs.getString(8);
                int fieldAvgLen = rs.getInt(9);


                HashMap<String, String> fieldMap = new HashMap();


                //  处理默认值
                // 排除空格 排除括号
                fieldDefaultStr = StrUtil.nullToEmpty(fieldDefaultStr).toUpperCase()
                        .replace(" ", "")
                        .replace(")", "")
                        .replace("(", "");
                // 排除 SYSDATE,SYS_GUID,USER
                if ("SYSDATE,SYS_GUID,USER".contains(fieldDefaultStr) || fieldDefaultStr.startsWith("NULL"))
                    fieldDefaultStr = "";
                fieldComment = StrUtil.nullToEmpty(fieldComment);


                //  VARCHAR2类型处理
                if (fieldType.equals("VARCHAR2") || fieldType.equals("NVARCHAR2")) {
                    fieldMap.put("fieldname", fieldName);
                    if (fixMode.equals("Y")) {
                        // 字符串字段太长导致建表报错 需缩短长度或转为TEXT
                        ResultSet rs2 = oracleConn.prepareStatement(String.format("select nvl(max(length(%s)),0)  from %s", fieldName, oracleTablename)).executeQuery();
                        if (rs2.next()) {
                            //暂定最长长度*2
                            int maxLength = rs2.getInt(1);

                            if (maxLength == 0) {
                                fieldLength = 50;
                            } else {
                                fieldLength = rs2.getInt(1) * 2;
                            }
                            fieldLength = fieldLength <= 50 ? 50 : fieldLength;
                        }
                        String baseType = "VARCHAR(%d)";
                        if (fieldLength > 2000)
                            baseType = "TEXT";
                        fieldMap.put("type", String.format(baseType, fieldLength));
                    } else {
                        fieldMap.put("type", String.format("VARCHAR(%d)", fieldLength));
                    }
                    fieldMap.put("primary", fieldName);
                    fieldMap.put("default", fieldDefaultStr);
                    fieldMap.put("isnull", fieldIsNull);
                    fieldMap.put("comment", fieldComment);
                } else if (fieldType.equals("CHAR")) {
                    //  CHAR类型
                    fieldMap.put("fieldname", fieldName);
                    fieldMap.put("type", String.format("CHAR(%d)", fieldLength));
                    fieldMap.put("primary", fieldName);
                    fieldMap.put("default", fieldDefaultStr);
                    fieldMap.put("isnull", fieldIsNull);
                    fieldMap.put("comment", fieldComment);
                } else if (fieldType.equals("UROWID")) {
                    //  UROWID类型
                    fieldMap.put("fieldname", fieldName);
                    fieldMap.put("type", String.format("VARCHAR(%d)", fieldLength));
                    fieldMap.put("primary", fieldName);
                    fieldMap.put("default", fieldDefaultStr);
                    fieldMap.put("isnull", fieldIsNull);
                    fieldMap.put("comment", fieldComment);
                } else if (fieldType.equals("DATE") || fieldType.equals("TIMESTAMP(6)") || fieldType.equals("TIMESTAMP(0)")) {
                    // 日期类型 映射成datetime
                    if (fieldDefaultStr.equals("sysdate") || fieldDefaultStr.equals("( (SYSDATE) )")) {
                        fieldMap.put("fieldname", fieldName);
                        fieldMap.put("type", "DATETIME");
                        fieldMap.put("primary", fieldName);
                        fieldMap.put("default", "current_timestamp()");
                        fieldMap.put("isnull", fieldIsNull);
                        fieldMap.put("comment", fieldComment);
                    } else {
                        fieldMap.put("fieldname", fieldName);
                        fieldMap.put("type", "DATETIME");
                        fieldMap.put("primary", fieldName);
                        fieldMap.put("default", "");
                        fieldMap.put("isnull", fieldIsNull);
                        fieldMap.put("comment", fieldComment);
                    }
                } else if (fieldType.equals("NUMBER")) {
                    // 数值类型

                    // 场景1:浮点类型判断，如number(5,2)映射为MySQL的DECIMAL(5,2)
                    // Oracle number(m,n) -> MySQL decimal(m,n)
                    if (fieldDataPricision > 0 && fieldDataScale > 0) {
                        fieldMap.put("fieldname", fieldName);
                        fieldMap.put("type", String.format("DECIMAL(%d,%d)", fieldDataPricision, fieldDataScale));
                        fieldMap.put("primary", fieldName);
                        fieldMap.put("default", fieldDefaultStr);
                        fieldMap.put("isnull", fieldIsNull);
                        fieldMap.put("comment", fieldComment);
                    } else if (fieldDataPricision > 0 && fieldDataScale == 0 && fieldAvgLen >= 6) {
                        // 场景2:整数类型以及平均字段长度判断，如number(20,0)，如果AVG_COL_LEN比较大，映射为MySQL的bigint
                        // fieldAvgLen >= 6 ,Oracle number(m,0) -> MySQL bigint

                        // number类型的默认值有3种情况，一种是null，一种是字符串值为null，剩余其他类型只提取默认值数字部分
                        if (fieldDefaultStr.equals("")) {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "BIGINT");
                            fieldMap.put("primary", fieldName);
                            fieldMap.put("default", fieldDefaultStr);
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        } else {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "BIGINT");
                            fieldMap.put("primary", fieldName);
                            //字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                            fieldMap.put("default", "'" + fieldDefaultStr
                                    .replace("(", "")
                                    .replace(")", "") +
                                    "'");
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        }
                    } else if (fieldDataPricision > 0 && fieldDataScale == 0 && fieldAvgLen < 6) {
                        // 场景3:整数类型以及平均字段长度判断，如number(10,0)，如果AVG_COL_LEN比较小，映射为MySQL的INT
                        // fieldAvgLen < 6 ,Oracle number(m,0) -> MySQL bigint
                        if (fieldDefaultStr.equals("")) {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "INT");
                            fieldMap.put("primary", fieldName);
                            fieldMap.put("default", fieldDefaultStr);
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        } else {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "INT");
                            fieldMap.put("primary", fieldName);
                            //字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                            fieldMap.put("default", "'" + fieldDefaultStr
                                    .replace("(", "")
                                    .replace(")", "")
                                    .replace("'", "") +
                                    "'");
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        }
                    } else if (fieldDataPricision == -1 && fieldDataScale == -1 && fieldAvgLen >= 6) {
                        // 场景4:无括号包围的number整数类型以及长度判断，如id number,若AVG_COL_LEN比较大，映射为MySQL的bigint
                        // fieldAvgLen >= 6 ,Oracle number -> MySQL bigint
                        if (fieldDefaultStr.equals("")) {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "BIGINT");
                            fieldMap.put("primary", fieldName);
                            fieldMap.put("default", fieldDefaultStr);
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        } else {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "BIGINT");
                            fieldMap.put("primary", fieldName);
                            //字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                            fieldMap.put("default", "'" + fieldDefaultStr
                                    .replace("(", "")
                                    .replace(")", "") +
                                    "'");
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        }
                    } else if (fieldDataPricision == -1 && fieldDataScale == -1 && fieldAvgLen < 6) {
                        // 场景5:无括号包围的number整数类型判断，如id number,若AVG_COL_LEN比较小，映射为MySQL的INT
                        // fieldAvgLen < 6 ,Oracle number -> MySQL int
                        if (fieldDefaultStr.equals("")) {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "INT");
                            fieldMap.put("primary", fieldName);
                            fieldMap.put("default", fieldDefaultStr);
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        } else {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "INT");
                            fieldMap.put("primary", fieldName);
                            //字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                            fieldMap.put("default", "'" + fieldDefaultStr
                                    .replace("(", "")
                                    .replace(")", "")
                                    .replace("'", "") +
                                    "'");
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        }
                    } else if (fieldDataPricision == -1 && fieldDataScale == 0 && fieldAvgLen >= 6) {
                        // 场景6:int整数类型判断，如id int,(oracle的int会自动转为number),若AVG_COL_LEN比较大，映射为MySQL的bigint
                        if (fieldDefaultStr.equals("")) {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "BIGINT");
                            fieldMap.put("primary", fieldName);
                            fieldMap.put("default", fieldDefaultStr);
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        } else {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "BIGINT");
                            fieldMap.put("primary", fieldName);
                            //字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                            fieldMap.put("default", "'" + fieldDefaultStr
                                    .replace("(", "")
                                    .replace(")", "")
                                    .replace("'", "") +
                                    "'");
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        }
                    } else if (fieldDataPricision == -1 && fieldDataScale == 0 && fieldAvgLen < 6) {
                        // 场景7:int整数类型判断，如id int,(oracle的int会自动转为number)若AVG_COL_LEN比较小，映射为MySQL的INT
                        if (fieldDefaultStr.equals("")) {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "INT");
                            fieldMap.put("primary", fieldName);
                            fieldMap.put("default", fieldDefaultStr);
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        } else {
                            fieldMap.put("fieldname", fieldName);
                            fieldMap.put("type", "INT");
                            fieldMap.put("primary", fieldName);
                            //字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                            fieldMap.put("default", "'" + fieldDefaultStr
                                    .replace("(", "")
                                    .replace(")", "")
                                    .replace("'", "") +
                                    "'");
                            fieldMap.put("isnull", fieldIsNull);
                            fieldMap.put("comment", fieldComment);
                        }
                    }


                } else
                    // 大字段映射规则，文本类型大字段映射为MySQL类型longtext,大字段不能有默认值，这里统一为null
                    if (fieldType.equals("CLOB") || fieldType.equals("NCLOB") || fieldType.equals("LONG")) {
                        fieldMap.put("fieldname", fieldName);
                        fieldMap.put("type", "LONGTEXT");
                        fieldMap.put("primary", fieldName);
                        //fieldMap.put("default", "NULL");
                        fieldMap.put("isnull", fieldIsNull);
                        fieldMap.put("comment", fieldComment);
                    } else if (fieldType.equals("BLOB") || fieldType.equals("RAW") || fieldType.equals("LONG RAW")) {
                        fieldMap.put("fieldname", fieldName);
                        fieldMap.put("type", "LONGBLOB");
                        fieldMap.put("primary", fieldName);
                        //fieldMap.put("default", "NULL");
                        fieldMap.put("isnull", fieldIsNull);
                        fieldMap.put("comment", fieldComment);
                    } else {
                        fieldMap.put("fieldname", fieldName);
                        fieldMap.put("type", fieldType + "(" + fieldLength + ")");
                        fieldMap.put("primary", fieldName);
                        //fieldMap.put("default", "NULL");
                        fieldMap.put("isnull", fieldIsNull);
                        fieldMap.put("comment", fieldComment);
                    }

                result.add(fieldMap);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        // 生成建表语句
        StringBuilder sql = new StringBuilder();
        sql.append("create table ");
        sql.append("`" + oracleTablename + "` (\n");
        int i = 0;
        for (HashMap field : result) {
            sql.append("`" + field.get("fieldname") + "`");
            sql.append(" " + field.get("type"));
//            if (!"".equals(field.get("default"))) {
//                sql.append(" " + "DEFAULT " + field.get("default"));
//            }
            if ("False".equals(field.get("isnull"))) {
                sql.append(" " + "NOT NULL");
            }
            if (!"".equals(field.get("comment"))) {
                sql.append(" comment '" + field.get("comment") + "'");
            }

            i++;
            if (i != result.size())
                sql.append(",\n");
        }

        sql.append("\n)");

        //System.out.println(sql);

        return sql.toString();
    }

    static String[] genIndex(String oracleTablename) {
        String sql = String.format("" +
                "SELECT\n" +
                "                           (CASE\n" +
                "                             WHEN C.CONSTRAINT_TYPE = 'P' OR C.CONSTRAINT_TYPE = 'R' THEN\n" +
                "                              'ALTER TABLE ' || T.TABLE_NAME || ' ADD CONSTRAINT ' ||\n" +
                "                              '`'||T.INDEX_NAME||'`' || (CASE\n" +
                "                                WHEN C.CONSTRAINT_TYPE = 'P' THEN\n" +
                "                                 ' PRIMARY KEY ('\n" +
                "                                ELSE\n" +
                "                                 ' FOREIGN KEY ('\n" +
                "                               END) || listagg('`'||T.COLUMN_NAME||'`',',') within group(order by T.COLUMN_position) || ');'\n" +
                "                             ELSE\n" +
                "                              'CREATE ' || (CASE\n" +
                "                                WHEN I.UNIQUENESS = 'UNIQUE' THEN\n" +
                "                                 I.UNIQUENESS || ' '\n" +
                "                                ELSE\n" +
                "                                 CASE\n" +
                "                                   WHEN I.INDEX_TYPE = 'NORMAL' THEN\n" +
                "                                    ''\n" +
                "                                   ELSE\n" +
                "                                    I.INDEX_TYPE || ' '\n" +
                "                                 END\n" +
                "                              END) || 'INDEX ' || '`'||T.INDEX_NAME||'`' || ' ON ' || T.TABLE_NAME || '(' ||\n" +
                "                              listagg('`'||T.COLUMN_NAME||'`',',') within group(order by T.COLUMN_position) || ');'\n" +
                "                           END) SQL_CMD\n" +
                "                      FROM USER_IND_COLUMNS T, USER_INDEXES I, USER_CONSTRAINTS C\n" +
                "                     WHERE T.INDEX_NAME = I.INDEX_NAME\n" +
                "                       AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)\n" +
                "                       AND T.TABLE_NAME = '%s'\n" +
                "                       and i.index_type != 'FUNCTION-BASED NORMAL'\n" +
                "                     GROUP BY T.TABLE_NAME,\n" +
                "                              T.INDEX_NAME,\n" +
                "                              I.UNIQUENESS,\n" +
                "                              I.INDEX_TYPE,\n" +
                "                              C.CONSTRAINT_TYPE", oracleTablename);
        String result = "";
        try (Connection oracleConn = DBUtils.getOracleConnection()) {
            ResultSet rs = oracleConn.prepareStatement(sql).executeQuery();

            while (rs.next()) {
                result = result + rs.getString(1);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.split(";");

    }
}
