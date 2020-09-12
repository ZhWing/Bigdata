package cn.zhw.MapReduce.test_19_3;

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

import com.alibaba.fastjson.JSONObject;

public class MapReduceLog2 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String string = value.toString();
			JSONObject line = JSONObject.parseObject(string);
			String common = line.getString("common");
			JSONObject josncommon = JSONObject.parseObject(common);
			
			// 构造函数中使用的是HashMap
			// 想要元素顺序和put的顺序相同，需要新建对象时指定为有序
			// 这样使用的就是LinkedHashMap，是有序的
			JSONObject jsonObject = new JSONObject(true);

			// "uid","platform","app_version","pid","cityid"
			String uid = josncommon.getString("uid");
			String platform = josncommon.getString("platform");
			String app_version = josncommon.getString("app_version");
			String pid = josncommon.getString("pid");
			String cityid = josncommon.getString("cityid");

			jsonObject.put("uid", uid);
			jsonObject.put("platform", platform);
			jsonObject.put("app_version", app_version);
			jsonObject.put("pid", pid);
			jsonObject.put("cityid", cityid);

			String out = jsonObject.toString();
			context.write(new Text(out), NullWritable.get());
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// 设置配置参数,使用默认参数
		Configuration conf = new Configuration();
		// 创建任务
		Job job = Job.getInstance(conf, MapReduceLog2.class.getSimpleName());
		// 指定 jar 文件
		job.setJarByClass(MapReduceLog2.class);
		// 指定数据的输入路径,指定第一个路径是 hdfs 的输入路径
		FileInputFormat.addInputPath(job, new Path("data/log.log"));

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

		// 指定输出路径
		FileOutputFormat.setOutputPath(job, new Path("data/006"));

		// 提交任务, 如果数 true 会返回任务执行的速度信息
		job.waitForCompletion(true);
	}
}
