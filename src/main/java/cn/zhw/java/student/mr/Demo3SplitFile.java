package cn.zhw.java.student.mr;

import java.io.*;

public class Demo3SplitFile {
    /**
     * 拆分文件
     *
     * 1、计算文件的总行数
     * 2、计算每个文件的数据行数
     * 3、拆分文件
     */

    /**
     * 统计文件的行数
     * @param fileName
     * @return
     */
    public static Long fileLength(String fileName) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));

        Long fileLength = 0L;
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            fileLength++;
        }
        return fileLength;
    }

    public static void main(String[] args) throws IOException {

        long fileLength = fileLength("D:\\兰智数加\\Project\\BigData\\data\\big_students.txt");
        int size = 8;
        long fileRow = fileLength / size;

        /**
         * 拆分文件
         * 每循环 fileRow 一次生成一个新的文件
         */
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader("D:\\兰智数加\\Project\\BigData\\data\\big_students.txt"));

        int fileFlag = 0;

        BufferedWriter bufferedWriter = new BufferedWriter(
                new FileWriter("D:\\兰智数加\\Project\\BigData\\data\\tmp\\part_" + fileFlag + ".txt"));

        long flag = 0;

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            flag++;

            if (flag > fileRow) {
                bufferedWriter.flush();
                bufferedWriter.close();

                fileFlag++;

                bufferedWriter = new BufferedWriter(
                        new FileWriter("D:\\兰智数加\\Project\\BigData\\data\\tmp\\part_" + fileFlag + ".txt"));
                flag = 0;
            }

            bufferedWriter.write(line);
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }
}
