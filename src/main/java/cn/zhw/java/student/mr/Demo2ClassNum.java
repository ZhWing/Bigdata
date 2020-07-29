package cn.zhw.java.student.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Demo2ClassNum {
    /**
     * 统计每个班级学生的人数
     * 1、读取数据
     * 2、切分数据
     */
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();

        BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\兰智数加\\Project\\BigData\\data\\big_students.txt"));

        String line;
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        // 统计每个班的人数，结果储存在 hashMap 中
        while ((line = bufferedReader.readLine()) != null) {
            String c = line.split(",")[4];
            Integer integer = hashMap.get(c);

            if (integer == null) {
                hashMap.put(c, 1);
            } else {
                hashMap.put(c, integer + 1);
            }
        }

        bufferedReader.close();
        System.out.println(hashMap);

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
