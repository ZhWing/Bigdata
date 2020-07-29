package cn.zhw.java.student;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Demo3ScoreSum {
    /**
     *
     * 计算学生总分，返回学生基本信息
     *
     */
    public static void main(String[] args) throws IOException {
        FileReader fileReader = new FileReader("D:\\兰智数加\\Project\\BigData\\data\\score.txt");
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split(",");
            String id = split[0];
            int score = Integer.parseInt(split[2]);

            /**
             *
             * 统计学生总分
             * 存在直接存进去
             * 不存在加上当前科目再存进去
             *
             */
            Integer sumScore = hashMap.get(id);
            if (sumScore == null) {
                hashMap.put(id, score);
            } else {
                hashMap.put(id, sumScore + score);
            }
        }

        /**
         *
         * 读取学生表
         * java 中通过 hashmap 实现分组求和和表关联
         *
         */

        FileReader fileReader1 = new FileReader("D:\\兰智数加\\Project\\BigData\\data\\students.txt");
        BufferedReader bufferedReader1 = new BufferedReader(fileReader1);

        String student;
        while ((student = bufferedReader1.readLine()) != null) {
            String id = student.split(",")[0];

            // 获取学生总分
            Integer sumScore = hashMap.get(id);
            System.out.println(student + "," + sumScore);
        }
    }
}
