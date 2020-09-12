package cn.zhw.MapReduce.test_19_3;

import java.io.IOException;

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

public class MapReduceLog {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String string = value.toString();
			if (string.indexOf("uid") != -1 && string.indexOf("platform") != -1 && string.indexOf("app_version") != -1
					&& string.indexOf("pid") != -1) {
				string = string.replaceAll("\"locationcity\":0", "\"locationcity\":1");
			}
			context.write(new Text(string), NullWritable.get());
		}
	}

	public static class MyReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (NullWritable nullWritable : values) {
				context.write(key, nullWritable);
			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// 设置配置参数,使用默认参数
		Configuration conf = new Configuration();
		// 创建任务
		Job job = Job.getInstance(conf, MapReduceLog.class.getSimpleName());
		// 指定 jar 文件
		job.setJarByClass(MapReduceLog.class);
		// 指定数据的输入路径,指定第一个路径是 hdfs 的输入路径
		FileInputFormat.addInputPath(job, new Path("data/log.log"));

		// 指定 map 的类
		job.setMapperClass(MyMapper.class);
		// 指定 map 输出的 key 和 value 的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 指定 reduce 类
		job.setReducerClass(MyReduce.class);
		// 指定 reduce 输出的 key 和 value 的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

//		// 使用自定义的分区组件
//		job.setPartitionerClass(CoursePartitioner.class);
//		// 和自定义分区组件同时使用，根据分区的个数设置reduceTask的个数
//		job.setNumReduceTasks(4);

		// 指定输出路径
		FileOutputFormat.setOutputPath(job, new Path("data/005"));

		// 提交任务, 如果数 true 会返回任务执行的速度信息
		job.waitForCompletion(true);
	}
}
