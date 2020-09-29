package cn.zhw.MapReduce.test_18_2;

import java.io.IOException;
import java.text.DecimalFormat;

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

/**
 * 统计每门课程的参考人数和课程平均分
 * 考虑到要需求要根据课程进行分组并对平均值进行排序，这里使用自定义bean的形式来进行处理
 * 因为要将数据根据课程进行分区并写入到不容的文件中，所以这里使用自定partitioner组件进行分区
 * 要注意的是这时候就要设置reduceTask的个数
 */
public class CourseTwo2 {
    static Text text = new Text();

    public static class MyMapper extends Mapper<LongWritable, Text, CourseBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] lines = value.toString().split(",");
            long sum = 0L;
            long totalTimes = lines.length - 2;
            for (int i = 2; i < lines.length; i++) {
                sum += Long.parseLong(lines[i]);
            }
            //格式化平均分使用，保留一位有效小数
            DecimalFormat df = new DecimalFormat(".0");
            //计算某个学生在某门课程中的平均分
            float avg = sum * 1.0f / totalTimes;
            String b = df.format(avg);
            //构建mapper输出的key
            CourseBean cb = new CourseBean(lines[0], lines[1], Float.parseFloat(b));

            context.write(cb, NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<CourseBean, NullWritable, CourseBean, NullWritable> {
        @Override
        protected void reduce(CourseBean key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            //因为自定义了分区组件，自定义类型有排序规则，所以这里直接输出就可以了
            for (NullWritable nullWritable : values) {
                context.write(key, nullWritable);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CourseTwo2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(CourseBean.class);
        job.setOutputValueClass(NullWritable.class);

        //使用自定义的分区组件
        job.setPartitionerClass(CoursePartitioner.class);
        //和自定义分区组件同时使用，根据分区的个数设置reduceTask的个数
        job.setNumReduceTasks(4);

        FileInputFormat.setInputPaths(job, new Path("/opt/ahdsjjs/input"));
        FileOutputFormat.setOutputPath(job, new Path("/opt/ahdsjjs /output3"));

        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone ? 0 : 1);
    }
}