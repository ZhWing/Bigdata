package cn.zhw.MapReduce.test_18_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CourseBean2 implements WritableComparable<CourseBean2> {
    private String course;
    private String name;
    private float avg;
    private long maxScore;

    public long getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(long maxScore) {
        this.maxScore = maxScore;
    }

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

    public CourseBean2(String course, String name, float avg, long maxScore) {
        super();
        this.course = course;
        this.name = name;
        this.avg = avg;
        this.maxScore = maxScore;
    }

    public CourseBean2() {
    }

    @Override
    public String toString() {
        return course + "\t" + name + "\t" + avg + "\t" + maxScore;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeUTF(name);
        out.writeFloat(avg);
        out.writeLong(maxScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        course = in.readUTF();
        name = in.readUTF();
        avg = in.readFloat();
        maxScore = in.readLong();
    }

    @Override
    public int compareTo(CourseBean2 o) {
      /*首先通过课程进行排序，课程相同的通过成绩进行排序
		值得一提的是，使用自定义分组组件指定的分组字段，一定要在comparaTo方法中使用字段得而前面
        eg: a
            a b
            a b c
            a b c d
            a b c d e  */
        int index = o.course.compareTo(this.course);
        if (index == 0) {
            long flag = o.maxScore - this.maxScore;
            return flag > 0L ? 1 : -1;

        } else {
            return index > 0L ? 1 : -1;
        }
    }

}
