package cn.zhw.java.reflect;

import java.util.ArrayList;

public class Test {
    public static void main(String[] args) {
        ArrayList<Student> students = DataRead.read("data\\students.txt", Student.class);
        for (Student student : students) {
            System.out.println(student);
        }

        ArrayList<Score> scores = DataRead.read("data\\score.txt", Score.class);
        for (Score score : scores) {
            System.out.println(score);
        }

        ArrayList<Cource> cources = DataRead.read("data\\cource.txt", Cource.class);
        for (Cource cource : cources) {
            System.out.println(cource);
        }
    }
}
