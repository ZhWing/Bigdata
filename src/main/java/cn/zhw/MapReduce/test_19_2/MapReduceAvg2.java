package cn.zhw.MapReduce.test_19_2;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceAvg2 {

	public static class MyMapper extends Mapper<LongWritable, Text, Student, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Student, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			long sum = 0L;
			for (int i = 2; i < line.length; i++) {
				sum += Long.parseLong(line[i]);
			}
			
			DecimalFormat df = new DecimalFormat(".0");
			float avg =  sum * 1.0f / (line.length - 2);
			float b = Float.parseFloat(df.format(avg));
			
			Student student = new Student(line[0], line[1], b);
			
			context.write(student, NullWritable.get());
		}
	}

	public static class MyReduce extends Reducer<Student, NullWritable, Student, NullWritable> {
		@Override
		protected void reduce(Student key, Iterable<NullWritable> values,
				Reducer<Student, NullWritable, Student, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (NullWritable value : values) {
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置配置参数,使用默认参数
		Configuration conf = new Configuration();
		// 创建任务
		Job job = Job.getInstance(conf, MapReduceAvg2.class.getSimpleName());
		// 指定 jar 文件
		job.setJarByClass(MapReduceAvg2.class);
		// 指定数据的输入路径,指定第一个路径是 hdfs 的输入路径
		FileInputFormat.addInputPath(job, new Path("data/studentScore.txt"));

		// 指定 map 的类
		job.setMapperClass(MyMapper.class);
		// 指定 map 输出的 key 和 value 的数据类型
		job.setMapOutputKeyClass(Student.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 指定 reduce 类
		job.setReducerClass(MyReduce.class);
		// 指定 reduce 输出的 key 和 value 的数据类型
		job.setOutputKeyClass(Student.class);
		job.setOutputValueClass(NullWritable.class);

		// 使用自定义的分区组件
		job.setPartitionerClass(CoursePartitioner.class);
		// 和自定义分区组件同时使用，根据分区的个数设置reduceTask的个数
		job.setNumReduceTasks(4);

		// 指定输出路径
		FileOutputFormat.setOutputPath(job, new Path("data/004"));

		// 提交任务, 如果数 true 会返回任务执行的速度信息
		job.waitForCompletion(true);
	}
}
