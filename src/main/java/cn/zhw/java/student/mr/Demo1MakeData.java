package cn.zhw.java.student.mr;

import java.io.*;

public class Demo1MakeData {
    /**
     * 将数据量变大
     */
    public static void main(String[] args) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\兰智数加\\Project\\BigData\\data\\students.txt"));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("D:\\兰智数加\\Project\\BigData\\data\\big_students.txt"));

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            for (int i = 0; i < 60000; i++) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        }

        bufferedReader.close();
        bufferedWriter.close();
    }
}
