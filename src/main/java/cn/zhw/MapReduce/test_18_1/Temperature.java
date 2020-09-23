package cn.zhw.MapReduce.test_18_1;

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

/**
 * 根据 data/Temperature.txt 编写 MapperReduce 脚本 获得每年的最高气温，并输出
 *
 * 数据格式如下：
 * 比如：2010012325 表示在 2010 年 01 月 23 日的气温为 25 度。
 * 现在要求使用 MapReduce，计算每一年出现过的最大气温
 */

public class Temperature {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String year = line.substring(0, 4);
            int temperature = Integer.parseInt(line.substring(8));
            System.out.println(year + ", " + temperature);
            context.write(new Text(year), new LongWritable(temperature));
        }
    }

    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long maxT = -1L;
            for (LongWritable value : values) {
                maxT = Math.max(maxT, value.get());
            }
            context.write(key, new LongWritable(maxT));
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Temperature.class.getSimpleName());

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, "data/Temperature.txt");
        FileOutputFormat.setOutputPath(job, new Path("data/011"));

        job.waitForCompletion(true);
    }
}
