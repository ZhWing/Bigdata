package cn.zhw.MapReduce.test_19_4;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MR_DnsLog2 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\\|");
			String www = split[1];
			context.write(new Text(www), new LongWritable(1));
		}
	}

	public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}

			context.write(key, new LongWritable(sum));
		}
	}

	private static class DescSort extends LongWritable.Comparator {

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			return -super.compare(a, b);
		}
	}

	public static void main(String[] args) throws Exception {

		Path outputPath = new Path("data/009");
		Configuration conf = new Configuration();
		Path tempDir = new Path("data/wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		Job job = Job.getInstance(conf, MR_DnsLog2.class.getSimpleName());
		job.setJarByClass(MR_DnsLog2.class);
		
		// 指定 map,Combiner, reduce 的类
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReduce.class);
		job.setReducerClass(MyReduce.class);
		
		// 指定 map 输出的 key 和 value 的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		// 指定 reduce 输出的 key 和 value 的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPaths(job, "data/dns_log.txt");
		
		// 先将词频统计任务的输出结果写到临时目录中, 下一个排序任务以临时目录为输入目录
		FileOutputFormat.setOutputPath(job, tempDir);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		if (job.waitForCompletion(true)) {
			Job sortJob = Job.getInstance(conf, "sortJob");
			sortJob.setJarByClass(MR_DnsLog2.class);

			FileInputFormat.addInputPath(sortJob, tempDir);
			// 实现 key 和 value 都可以自定义类，需要实现Writable接口
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);

			// InverseMapper由 hadoop 库提供，作用是实现map()之后的数据对的 key 和 value 交换
			sortJob.setMapperClass(InverseMapper.class);
		
			// job.setPartitionerClass(CoursePartitioner.class); // 使用自定义的分区组件
			// sortJob.setNumReduceTasks(1); // 将 Reducer 的个数限定为1, 最终输出的结果文件就是一个
			
			outputPath.getFileSystem(conf).delete(outputPath, true);
			FileOutputFormat.setOutputPath(sortJob, outputPath);

			sortJob.setOutputKeyClass(LongWritable.class);
			sortJob.setOutputValueClass(Text.class);
			/*
			 * hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。 因此我们实现了一个
			 * IntWritableDecreasingComparator 类, 并指定使用这个自定义的 DescSort 类对输出结果中的 key
			 * (词频)进行排序
			 */
			sortJob.setSortComparatorClass(DescSort.class);
			sortJob.waitForCompletion(true);
		}
	}
}
