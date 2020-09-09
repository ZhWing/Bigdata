package cn.zhw.MapReduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DnslogClear2 {

    public static Map<String, Integer> map = new LinkedHashMap<String, Integer>();

    public static class DnslogClearMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split("\\|");
            String str = arr[1];
            context.write(new Text(str), one);
        }
    }

    public static class DnslogClearReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        protected void reduce(Text key, Iterable<IntWritable> values, Context context)

                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {

                sum += val.get();//相当于，将两个1相加

            }

            result.set(sum);

            map.put(key.toString(), sum);

            context.write(key, result);//写出这个单词，和这个单词出现次数<单词，单词出现次数>


        }
    }

    private static class Comparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }


    public static void main(String[] args) throws Exception {

        Path outputPath = new Path("C:\\Users\\Administrator\\Desktop\\result5");

        Configuration conf = new Configuration();           //实例化配置文件类

        Path tempDir = new Path("wordcount-temp-" + Integer.toString(
                new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录

        Job job = new Job(conf, "word count");
        job.setJarByClass(DnslogClear2.class);
        try {
            job.setMapperClass(DnslogClearMapper2.class);
            job.setCombinerClass(DnslogClearReducer2.class);
            job.setReducerClass(DnslogClearReducer2.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPaths(job, "C:\\Users\\Administrator\\Desktop\\dns_log.txt");
            FileOutputFormat.setOutputPath(job, tempDir);//先将词频统计任务的输出结果写到临时目
            //录中, 下一个排序任务以临时目录为输入目录。
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            if (job.waitForCompletion(true)) {
                Job sortJob = new Job(conf, "sort");
                sortJob.setJarByClass(DnslogClear2.class);

                FileInputFormat.addInputPath(sortJob, tempDir);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);

                /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
                sortJob.setMapperClass(InverseMapper.class);
                /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。*/
                sortJob.setNumReduceTasks(1);
                outputPath.getFileSystem(conf).delete(outputPath, true);
                FileOutputFormat.setOutputPath(sortJob, outputPath);

                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
                /*Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。
                 * 因此我们实现了一个 IntWritableDecreasingComparator 类,
                 * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序*/
                sortJob.setSortComparatorClass(Comparator.class);
//	                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
                sortJob.waitForCompletion(true);
            }
        } finally {
            FileSystem.get(conf).deleteOnExit(tempDir);

            System.out.println("统计输出该原始数据集中网站被访问次数最多的前5名的网站地址和次数:");
            File file = new File("C:\\Users\\Administrator\\Desktop\\result5\\part-r-00000");
            BufferedReader b = null;
            try {
                FileInputStream rf = new FileInputStream(file);
                InputStreamReader bf = new InputStreamReader(rf);
                b = new BufferedReader(bf);
                String s = null;
                int index = 0;
                while ((s = b.readLine()) != null) {
                    if (index < 5)
                        System.out.println(s);
                    index++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                b.close();
            }
        }
    }
}
