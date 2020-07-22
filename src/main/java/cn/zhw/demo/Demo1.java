package cn.zhw.demo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

public class Demo1 {
    /**
     * 统计年级排名前十学生各科的分数
     * [学号,学生姓名，学生班级，科目名，分数]
     */

    public static void main(String[] args) throws IOException {

        // 计算学生总分
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader("D:\\兰智数加\\Project\\BigData\\data\\score.txt"));
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split(",");
            String id = split[0];
            int s = Integer.parseInt(split[2]);

            Integer integer = hashMap.get(id);
            if (integer == null) {
                hashMap.put(id, s);
            } else {
                hashMap.put(id, integer + s);
            }
        }

        // 取总分排名前十的学生
        ArrayList<String> top = new ArrayList<String>();
        for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
            String id = entry.getKey();
            Integer sumScore = entry.getValue();

            top.add(id + "," + sumScore);
        }

        // 排序
        top.sort(new Comparator<String>() {
            public int compare(String o1, String o2) {
                int s1 = Integer.parseInt(o1.split(",")[1]);
                int s2 = Integer.parseInt(o2.split(",")[1]);

                return s2 - s1;
            }
        });

        // 取前十
        List<String> top10 = top.subList(0, 10);
        ArrayList<String> newTop = new ArrayList<>();
        for (String s : top10) {
            String id = s.split(",")[0];
            newTop.add(id);
        }

        // 整理数据
        // 将学生表读取到内存中转换成一个 map 集合，Key = id， Value = message
        bufferedReader = new BufferedReader(
                new FileReader("D:\\兰智数加\\Project\\BigData\\data\\students.txt"));
        HashMap<String, String> studentMap = new HashMap<>();
        while ((line = bufferedReader.readLine()) != null) {
            String id = line.split(",")[0];
            studentMap.put(id, line);
        }

        bufferedReader = new BufferedReader(
                new FileReader("D:\\兰智数加\\Project\\BigData\\data\\cource.txt"));
        HashMap<String, String> courcetMap = new HashMap<>();
        while ((line = bufferedReader.readLine()) != null) {
            String cId = line.split(",")[0];
            String cName = line.split(",")[1];
            courcetMap.put(cId, cName);
        }

        // 关联学生表和分数表
        bufferedReader = new BufferedReader(
                new FileReader("D:\\兰智数加\\Project\\BigData\\data\\score.txt"));
        while ((line = bufferedReader.readLine()) != null) {
            String id = line.split(",")[0];
            String cource = line.split(",")[1];
            String score = line.split(",")[2];

            if (newTop.contains(id)) {
                // 通过学生 id 获取学生信息
                String stuInfo = studentMap.get(id);
                //  通过科目编号获取科目名称
                String cName = courcetMap.get(cource);

                System.out.println(stuInfo + "," + cName + "," + score);
            }
        }
    }
}
