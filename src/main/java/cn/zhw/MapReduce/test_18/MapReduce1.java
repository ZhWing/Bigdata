package cn.zhw.MapReduce.test_18;

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

public class MapReduce1 {

	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			if (!split[2].equals("null")) {
				if (split[4].equals("1")) {
					split[4] = "本系统登录";
				} else if (split[4].equals("0")) {
					split[4] = "第三方集成";
				}
				String line = "";
				for (String string : split) {
					line+= string + "\t";
				}
				context.write(key, new Text(line));
			}
		}
	}
	
	public static class MyReduce extends Reducer<LongWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// 设置配置参数,使用默认参数
        Configuration conf = new Configuration();
        // 创建任务
        Job job = Job.getInstance(conf, MapReduce1.class.getSimpleName());
        // 指定 jar 文件
        job.setJarByClass(MapReduce1.class);
        // 指定数据的输入路径,指定第一个路径是 hdfs 的输入路径
        FileInputFormat.addInputPath(job, new Path("data/access.log"));

        // 指定 map 的类
        job.setMapperClass(MyMapper.class);
        // 指定 map 输出的 key 和 value 的数据类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 指定 reduce 类
        job.setReducerClass(MyReduce.class);
        // 指定 reduce 输出的 key 和 value 的数据类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 指定输出路径
        FileOutputFormat.setOutputPath(job, new Path("data/001"));

        // 提交任务, 如果数 true 会返回任务执行的速度信息
        job.waitForCompletion(true);
	}

}
