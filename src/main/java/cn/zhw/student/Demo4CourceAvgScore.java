package cn.zhw.student;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Demo4CourceAvgScore {
    /**
     * 计算每个学科的平均分
     */
    public static void main(String[] args) throws IOException {
        FileReader fileReader = new FileReader("D:\\兰智数加\\Project\\BigData\\data\\score.txt");
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        // 各个学科平均分
        HashMap<String, Integer> sumScore = new HashMap<String, Integer>();
        // 各个学科人数
        HashMap<String, Integer> sumNum = new HashMap<String, Integer>();

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            // 学科
            String cource = line.split(",")[1];
            // 分数
            int score = Integer.parseInt(line.split(",")[2]);

            /**
             * 1、统计每个学科总分
             * 2、统计每个学科人数
             */

            // 总分
            Integer integer = sumScore.get(cource);
            if (integer == null) {
                sumScore.put(cource, score);
            } else {
                sumScore.put(cource, integer + score);
            }
            // 总人数
            Integer num = sumNum.get(cource);
            if (integer == null) {
                sumNum.put(cource, 1);
            } else {
                sumNum.put(cource, num + 1);
            }
        }

        BufferedReader reader = new BufferedReader(new FileReader("D:\\兰智数加\\Project\\BigData\\data\\cource.txt"));
        HashMap<String, String> hashMap = new HashMap<String, String>();
        String line1;
        while ((line1 = reader.readLine()) != null) {
            String id = line1.split(",")[0];
            String cource = line1.split(",")[1];
            hashMap.put(id, cource);
        }

        for (Map.Entry<String, Integer> entry : sumScore.entrySet()) {
            String cource = entry.getKey();
            Integer score = entry.getValue();

            // 获取总人数
            Integer num = sumNum.get(cource);

            double avg = score / (double)num;

            // 通过学科编号关联学科名称
            String name = hashMap.get(cource);
            System.out.println(name + "," + avg);
        }
    }
}
