package cn.zhw.java.student;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Demo2ClassGenderNum {

    /**
     * 统计每个班级每种性别学生人数
     *
     * 1、读取数据
     * 2、切分数据
     */
    public static void main(String[] args) throws IOException {
        FileReader fileReader = new FileReader("D:\\兰智数加\\Project\\BigData\\data\\students.txt");
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        ArrayList<String> lists = new ArrayList<String>();

        // 切分出 班级 性别
        while ((line = bufferedReader.readLine()) != null) {
            String list = line.split(",")[4];
            String gender = line.split(",")[3];
            lists.add(list + "," + gender);
        }

        bufferedReader.close();

        // 统计么每个班男女生数量
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();


        for (String list : lists) {
            // 判断是否已经存在 hashMap 中
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
