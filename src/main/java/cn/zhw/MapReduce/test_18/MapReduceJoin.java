package cn.zhw.MapReduce.test_18;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Vector;

public class MapReduceJoin {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 // 获取切片路径信息
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String path = inputSplit.getPath().toString();
            
            if (path.contains("ip")) {
            	String line = value.toString();
            	String[] split = line.split("\t");
            	String cip = split[0];
            	
            	String ip_info = "ip" + "\t" + line;
            	context.write(new Text(cip), new Text(ip_info));
            }
            
            if (path.contains("access")) {
            	String line = value.toString();
            	String[] split = line.split("\t");
            	
            	String lip = split[1];
            	String log_inifo = "access" + "\t" +  line;
            	context.write(new Text(lip), new Text(log_inifo));
            }
		}
	}
	
	public static class MyReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Vector<String> logs = new Vector<String>();
			Vector<String> citys = new Vector<String>();
			
			for (Text value : values) {
				String[] line = value.toString().split("\t");
				
				if (line[0].equals("access")) {
					String log_inifo = "";
					for (int i = 2; i < line.length; i++) {
						log_inifo += line[i] + "\t";
					}
					// System.out.println(log_inifo);
					logs.add(log_inifo);
				}
				if (line[0].equals("ip")) {
					String ip_inifo = "";
					for (int i = 2; i < line.length; i++) {
						ip_inifo += line[i] + "\t";
					}
					// System.out.println(ip_inifo);
					citys.add(ip_inifo);
				}
			}
			for (String city : citys) {
				for (String log : logs) {
					context.write(new Text(log + "\t"), new Text(city));
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
        // conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, MapReduceJoin.class.getSimpleName());
        job.setJarByClass(MapReduceJoin.class);
        FileInputFormat.addInputPath(job, new Path("data/access.log"));
        FileInputFormat.addInputPath(job, new Path("data/ip.txt"));

        // 指定 map 类
        job.setMapperClass(MyMapper.class);
        // 指定 map 输出的 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 指定 reduce 类
        job.setReducerClass(MyReduce.class);
        // 指定 reduce 输出的 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 指定输出路径
        FileOutputFormat.setOutputPath(job, new Path("data/002"));

        // 提交任务, 如果数 true 会返回任务执行的速度信息
        job.waitForCompletion(true);
	}

}
