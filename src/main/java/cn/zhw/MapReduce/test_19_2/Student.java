package cn.zhw.MapReduce.test_19_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Student implements WritableComparable<Student> {
	private String course;
	private String name;
	private float avg;

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

	public Student() {

	}

	public Student(String course, String name, Float avg) {
		super();
		this.course = course;
		this.name = name;
		this.avg = avg;
	}

	@Override
	public String toString() {
		return course + "," + name + "," + avg;
	}

	/**
	 ** 反序列化
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		course = in.readUTF();
		name = in.readUTF();
		avg = in.readFloat();

	}

	/**
	 ** 序列化
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(course);
		out.writeUTF(name);
		out.writeDouble(avg);
	}

	// 比较规则
	@Override
	public int compareTo(Student o) {
		// TODO Auto-generated method stub
		double flag = o.avg - this.avg;
		return flag > 0.0f ? 1 : -1;
	}

}
