package cn.zhw.student.mr;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Demo4Map {

    /**
     * 为每一个文件启动一个线程处理数据
     */
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        // 创建进程池
        ExecutorService threadPool = Executors.newFixedThreadPool(8);

        // 获取文件列表
        File file = new File("D:\\兰智数加\\Project\\BigData\\data\\tmp");
        File[] files = file.listFiles();

        int flag = 0;
        for (File file1 : files) {
            MapTask mapTask = new MapTask(file1, flag);
            threadPool.submit(mapTask);
            flag++;
        }

        //关闭线程池
        threadPool.shutdown();

        //等待关闭
        threadPool.awaitTermination(100000L, TimeUnit.SECONDS);

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}

class MapTask extends Thread {

    private File file;
    private int flag;

    public MapTask(File file, int flag) {
        this.file = file;
        this.flag = flag;
    }

    @Override
    public void run() {
        try {
            BufferedReader bufferedReader = new BufferedReader(
                    new FileReader(file));
            HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String s = line.split(",")[4];

                Integer integer = hashMap.get(s);
                if (integer == null) {
                    hashMap.put(s, 1);
                } else {
                    hashMap.put(s, integer + 1);
                }
            }

            BufferedWriter bufferedWriter = new BufferedWriter(
                    new FileWriter("D:\\兰智数加\\Project\\BigData\\data\\tmp2\\part_" + flag + ".txt"));

            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();

                bufferedWriter.write(key + "," + value);
                bufferedWriter.newLine();
            }
            bufferedReader.close();
            bufferedWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
