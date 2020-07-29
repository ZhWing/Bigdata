package cn.zhw.java.student.mr;

import java.io.*;
import java.util.HashMap;

public class Demo5Reduce {
    public static void main(String[] args) throws IOException {
        // 获取文件
        File file = new File("D:\\兰智数加\\Project\\BigData\\data\\tmp2");
        File[] files = file.listFiles();

        HashMap<String, Long> hashMap = new HashMap<String, Long>();
        for (File file1 : files) {
            BufferedReader bufferedReader = new BufferedReader(
                    new FileReader(file1));

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split(",");
                // 班级
                String c = split[0];
                // 人数
                long num = Long.parseLong(split[1]);

                Long aLong = hashMap.get(c);
                if (aLong == null) {
                    hashMap.put(c, num);
                } else {
                    hashMap.put(c, aLong + num);
                }
            }
            bufferedReader.close();
        }
        System.out.println(hashMap);
    }
}
