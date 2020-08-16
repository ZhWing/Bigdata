package cn.zhw.MapReduce.DianXin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DianXinAvg {

    public static class DianXinMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            if (split.length == 8 && split[4] != null) {
                if (split[4].equals("\\N")) {
                    split[4] = "0";
                }
                context.write(new Text(split[0]), new LongWritable(Integer.parseInt(split[4])));
            }
        }
    }

    public static class DianXinReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0L;
            long count = 0L;
            for (LongWritable value : values) {
                sum += value.get();
                count++;
            }
            context.write(key, new LongWritable(sum / count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置配置参数,使用默认参数
        Configuration conf = new Configuration();
        // 创建任务
        Job job = Job.getInstance(conf, DianXinAvg.class.getSimpleName());
        // 指定 jar 文件
        job.setJarByClass(DianXinAvg.class);
        // 指定数据的输入路径,指定第一个路径是 hdfs 的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 指定 map 的类
        job.setMapperClass(DianXinMapper.class);
        // 指定 map 输出的 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 指定 reduce 类
        job.setReducerClass(DianXinReduce.class);
        // 指定 reduce 输出的 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 指定输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务, 如果数 true 会返回任务执行的速度信息
        job.waitForCompletion(true);
    }
}
