package cn.zhw.MapReduce.test_19_4;

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

import java.io.IOException;

/**
 * 统计输出该原始数据集中网站被访问次数最多的前5名的网站地址和次数。
 */
public class DnsLog3 {


    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\\|");
            String ip = split[1];
            context.write(new Text(ip), new LongWritable(1));
        }
    }

    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }

            context.write(key, new LongWritable(count));
        }
    }

    private static class DescSort extends LongWritable.Comparator {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MRJob");

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPaths(job, "data/dns_log.txt");
        FileOutputFormat.setOutputPath(job, new Path("data/out/tmp"));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (job.waitForCompletion(true)) {
            Job sortJob = Job.getInstance(conf, "sortJob");

            // 实现 key 和 value 都可以自定义类，需要实现Writable接口
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);

            // InverseMapper由 hadoop 库提供，作用是实现map()之后的数据对的 key 和 value 交换
            sortJob.setMapperClass(InverseMapper.class);
            sortJob.setOutputKeyClass(LongWritable.class);
            sortJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPaths(sortJob, "data/tmp");
            FileOutputFormat.setOutputPath(sortJob, new Path("data/out/001"));

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
