package cn.zhw.MapReduce.test_18_2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组组件
 * 1、如果没有定义自定义的分组组件，默认的使用compareTo方法中的字段进行分组排序
 * 这里要继承WritableComparator类，来进行序列化和比较
 */
public class CourseGroupComparator extends WritableComparator {

    /**
     * 为了解决下面出现空指针的现象，所以在类中声明一个构造函数来进行创建
     */
    public CourseGroupComparator() {
        super(CourseBean2.class, true);
    }


    /**
     * 如果直接这样使用会出现一个空指针的错误，主要是a,b没有进行构造，所以是空的；
     * 创建一个构造方法就可以进行解决
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CourseBean2 cb1 = (CourseBean2) a;
        CourseBean2 cb2 = (CourseBean2) b;
        //这里是根据课程名称进行处理的
        return cb1.getCourse().compareTo(cb2.getCourse());
    }
}
