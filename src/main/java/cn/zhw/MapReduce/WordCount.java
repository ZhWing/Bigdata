package cn.zhw.MapReduce;

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

public class WordCount {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] split = line.split(",");

            for (String word : split) {
                context.write(new Text(word), new LongWritable(1L));
            }
        }
    }

    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0L;

            for (LongWritable value : values) {
                count += value.get();
            }

            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, WordCount.class.getSimpleName());

        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MyMapper.class);
        // 指定 map 输出的 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 指定 reduce 类
        job.setReducerClass(MyReduce.class);
        // 指定 reduce 输出的 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 指定输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务, 如果数 true 会返回任务执行的速度信息
        job.waitForCompletion(true);
    }
}
