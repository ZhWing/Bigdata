package cn.zhw.MapReduce.test_18_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CourseBean implements WritableComparable<CourseBean> {
    private String course; //课程名
    private String name; //学生姓名
    private float avg; //平均分

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getAvg() {
        return avg;
    }

    public void setAvg(float avg) {
        this.avg = avg;
    }

    public CourseBean(String course, String name, float avg) {
        super();
        this.course = course;
        this.name = name;
        this.avg = avg;
    }

    public CourseBean() {
    }

    /**
     * 通过toString方法自定义输出类型
     */
    @Override
    public String toString() {
        return course + "\t" + name + "\t" + avg;
    }

    /**
     * 序列化
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeUTF(name);
        out.writeFloat(avg);
    }

    /**
     * 反序列化
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        course = in.readUTF();
        name = in.readUTF();
        avg = in.readFloat();
    }

    //比较规则
    @Override
    public int compareTo(CourseBean o) {
        float flag = o.avg - this.avg;
        return flag > 0.0f ? 1 : -1;
    }
}
