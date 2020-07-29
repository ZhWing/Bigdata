package cn.zhw.java.reflect;

import java.io.IOException;

public class Demo1Calss {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        /**
         * 所在class文件被加载到内存中之后都是Class的一个对象（类对象）
         * 每一个类再内存中有且只有一个唯一的内对象
         */

        Student student = new Student();
        student.setId("001");

        Class<?> aClass = Class.forName("cn.zhw.java.reflect.Student");
    }
}
