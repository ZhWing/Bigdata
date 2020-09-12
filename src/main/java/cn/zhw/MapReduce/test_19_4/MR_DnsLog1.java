package cn.zhw.MapReduce.test_19_4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR_DnsLog1 {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("//|");
			if (split.length >= 5) {
				line = line.replaceAll("www.taobao.com", "ShoppingAction ");
				context.write(new Text(line), NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置配置参数,使用默认参数
		Configuration conf = new Configuration();
		// 创建任务
		Job job = Job.getInstance(conf, MR_DnsLog1.class.getSimpleName());
		// 指定 jar 文件
		job.setJarByClass(MR_DnsLog1.class);
		// 指定数据的输入路径,指定第一个路径是 hdfs 的输入路径
		FileInputFormat.addInputPath(job, new Path("data/dns_log.txt"));

		// 指定 map 的类
		job.setMapperClass(MyMapper.class);
		// 指定 map 输出的 key 和 value 的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

//		// 指定 reduce 类
//		job.setReducerClass(MyReduce.class);
//		// 指定 reduce 输出的 key 和 value 的数据类型
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(NullWritable.class);

//		// 使用自定义的分区组件
//		job.setPartitionerClass(CoursePartitioner.class);
//		// 和自定义分区组件同时使用，根据分区的个数设置reduceTask的个数
//		job.setNumReduceTasks(4);

		// 指定输出路径
		FileOutputFormat.setOutputPath(job, new Path("data/008"));

		// 提交任务, 如果数 true 会返回任务执行的速度信息
		job.waitForCompletion(true);
	}
}
