package oracle2mysql;

import cn.hutool.core.io.file.FileReader;
import oracle2mysql.worker.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Oracle2mysqlApplication {

    public static void main(String[] args) throws Exception {
        String parameter = Arrays.toString(args).toLowerCase();
        boolean isCompareDB = parameter.contains("-compare");
        boolean isdatatofile = parameter.contains("-datatofile");
        boolean isfiletodata = parameter.contains("-filetodata");
        boolean isConvertDDL = parameter.contains("-convertddl");
        boolean isfixautoincrement = parameter.contains("-fixautoincrement");
        String tableList = "tableList.txt";
        int parallel = 2;

        for (int i = 0; i < args.length; i++) {
            if (args[i].toLowerCase().equals("-p")) {
                parallel = Integer.valueOf(args[i + 1]);
            }
        }

        System.out.println("并行数量：" + parallel + "   如未指定-p 则默认2个并行");

        // 获取需要执行的表
        FileReader fileReader = new FileReader(System.getProperty("user.dir") + "/" + tableList);
        List<String> list = fileReader.readLines();

        int taskCount = list.size();
        long start = System.currentTimeMillis();
        long end;

        CountDownLatch countDownLatch = new CountDownLatch(taskCount);
        ExecutorService pool = Executors.newFixedThreadPool(parallel);
        for (String table : list) {

            pool.execute(() -> {
                try {
                    if (isCompareDB) {
                        WorkerCompare.run(table);
                    } else if (isdatatofile) {
                        WorkerOracle2Txt.run(table);
                    } else if (isfiletodata) {
                        WorkerTxt2Mysql.run(table);
                    } else if (isConvertDDL) {
                        WorkerConvertDDL.run(table);
                    } else if (isfixautoincrement) {
                        WorkerFixAutoIncrement.run(table);
                    } else {
                        System.out.println("无命令运行 需指定要运行的命令");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
                // 当前进度
                System.out.print(
                        "正在执行: " + (taskCount - countDownLatch.getCount()) + "/" + taskCount + " " + table + "\r");
            });
        }
        countDownLatch.await();
        end = System.currentTimeMillis();
        System.out.println(String.format("\n总体耗时: %.2f 分钟 ", (end - start) / 1000 / 60.01));
        pool.shutdown();

    }
}
