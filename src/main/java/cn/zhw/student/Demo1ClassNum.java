package cn.zhw.student;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Demo1ClassNum {
    /**
     *
     * 统计每个班学生人数
     * 1、读取数据
     * 2、切分数据
     *
     */

    public static void main(String[] args) throws IOException {
        FileReader fileReader = new FileReader("D:\\兰智数加\\Project\\BigData\\data\\students.txt");
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;

        // 存储所有的班级
        ArrayList<String> lists = new ArrayList<String>();

        while ((line = bufferedReader.readLine()) != null) {
            String list = line.split(",")[4];
            lists.add(list);
        }
        bufferedReader.close();

        // 储存计算结果
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        for (String list : lists) {

            // 判断班级是否存在与 hashMap 中
            // 存在返回人数，不存在返回 null
            Integer integer = hashMap.get(list);

            if (integer == null) {
                hashMap.put(list, 1);
            } else {
                hashMap.put(list, integer + 1);
            }
        }
        System.out.println(hashMap);
    }
}
