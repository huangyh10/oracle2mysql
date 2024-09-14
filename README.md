# oracle2mysql

oracle 到 mysql 迁移 Java 实现 只支持表结构和数据
已测试 oracle 11g mysql 8.0

实现核心逻辑 ddl 转换 > oracle 抽数到文本 > mysql 从文本导入

DDL 关键转换逻辑抄的 https://github.com/iverycd/oracle_to_mysql
主要解决了超大数据量效率慢和容易 OOM 的问题

使用方式:
tableList.txt 填入要迁移的表名
config.ini 修改配置

-compare 比对双边表行数
-datatofile oracle 数据导出到 txt
-filetodata txt 文件导入 mysql(load data local)
-convertddl oracle 表结构转 mysql
-fixautoincrement 尝试修复标自增（仅数字 id 列）
-p X X 个并行数
