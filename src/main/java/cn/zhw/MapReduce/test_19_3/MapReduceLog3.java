package cn.zhw.MapReduce.test_19_3;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class MapReduceLog3 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 获取切片路径信息
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String path = inputSplit.getPath().toString();
            
            String line = value.toString();
            
            if (path.indexOf("part-r-00000") != -1) {
            	JSONObject city = JSON.parseObject(line);
            	String cityid = city.getString("cityid");
				
            	String log_info = "log" + "\t" + line;
            	
            	if (!"".equals(cityid)) {
            		// System.out.println(cityid + "," + log_info);
            		context.write(new Text(cityid), new Text(log_info));
            	}
            	
            }
            
            if (path.indexOf("city") != -1) {
            	String[] split = line.split("\\|");
            	String cid = split[0];
            	String city_info = "city" + "\t" + split[5];
            	if (!"null".equals(split[5])) {
            		// System.out.println(cid + "," + city_info);
                	context.write(new Text(cid), new Text(city_info));
            	}
            }
		}
	}
	
	public static class MyReduce extends Reducer<Text, Text, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Vector<String> logs = new Vector<String>();
			Vector<String> citys = new Vector<String>();
			
			for (Text value : values) {
				String[] line = value.toString().split("\t");
				
				if (line[0].equals("log")) {
					// System.out.println(line[1]);
					logs.add(line[1]);
				}
				if (line[0].equals("city")) {
					// System.out.println(line[1]);
					citys.add(line[1]);
				}
			}
			
			
			for (String log : logs) {
				for (String city : citys) {
					// System.out.println(log);
					JSONObject jsonlog = JSON.parseObject(log);
					JSONObject out = new JSONObject(true);
					
					String uid = jsonlog.getString("uid");
	            	String platform = jsonlog.getString("platform");
					String app_version = jsonlog.getString("app_version");
					String pid = jsonlog.getString("pid");
					
					out.put("uid", uid);
					out.put("platform", platform);
					out.put("app_version", app_version);
					out.put("pid", pid);
					out.put("cityid", city);
					
					// System.out.println(out.toString());
					context.write(new Text(out.toString()), NullWritable.get());
				}
			}
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// 设置配置参数,使用默认参数
				Configuration conf = new Configuration();
				// 创建任务
				Job job = Job.getInstance(conf, MapReduceLog3.class.getSimpleName());
				// 指定 jar 文件
				job.setJarByClass(MapReduceLog3.class);
				// 指定数据的输入路径,指定第一个路径是 hdfs 的输入路径
				FileInputFormat.addInputPath(job, new Path("data/006/part-r-00000"));
				FileInputFormat.addInputPath(job, new Path("data/city.txt"));

				// 指定 map 的类
				job.setMapperClass(MyMapper.class);
				// 指定 map 输出的 key 和 value 的数据类型
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);

				// 指定 reduce 类
				job.setReducerClass(MyReduce.class);
				// 指定 reduce 输出的 key 和 value 的数据类型
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);

				// 指定输出路径
				FileOutputFormat.setOutputPath(job, new Path("data/007"));

				// 提交任务, 如果数 true 会返回任务执行的速度信息
				job.waitForCompletion(true);
	}
}
