package cn.zhw.MapReduce.DianXin;

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

public class DianXinJoin {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, DianXinJoin.class.getSimpleName());
        job.setJarByClass(DianXinJoin.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        // 指定 map 类
        job.setMapperClass(DianXinMapper.class);
        // 指定 map 输出的 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 指定 reduce 类
        job.setReducerClass(DianxinReduce.class);
        // 指定 reduce 输出的 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 指定输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // 提交任务, 如果数 true 会返回任务执行的速度信息
        job.waitForCompletion(true);
    }

    public static class DianXinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // 获取切片路径信息
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String path = inputSplit.getPath().toString();

            // 打标签 区分连个输入路径的数据
            // 1、判断是否是 dianxin_data 数据
            if (path.contains("dianxin_data")) {
                String line = value.toString();
                String[] split = line.split("\t");
                if (split.length == 8 && split[2] != null) {
                    // 将城市 id 作为 key
                    String id = split[2]; // Key

                    // 将 value 打上标签
                    String data_info = "dianxin_data" + "," + split[0] + "," + split[4];
                    // dianxin_data,D55433A437AEC8D8D3DB2BCA56E9E64392A9D93C,301
                    context.write(new Text(id), new Text(data_info));
                }
            }
            // 给 city_id 的数据打标
            if (path.contains("city_id")) {
                String line = value.toString();
                String[] split = line.split("\t");

                if (split.length == 2) {
                    //获取join的key，将城市id作为key
                    String cid = split[0];
                    // 将 value 打上标签
                    String city_info = "city_id" + "," + split[1];
                    // city_id,合肥市
                    context.write(new Text(cid), new Text(city_info));
                }
            }
        }
    }

    public static class DianxinReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // 83401 ({dianxin_data,D55433A437AEC8D8D3DB2BCA56E9E64392A9D93C,301}, {city_id,合肥市})
            Vector<String> datas = new Vector<String>();
            Vector<String> citys = new Vector<String>();

            for (Text value : values) {
                String line = value.toString();
                //判断开头标记，放入集合
                if (line.startsWith("dianxin_data")) {
                    datas.add(line.substring(12));
                }
                if (line.startsWith("city_id")) {
                    citys.add(line.substring(7));
                }
            }
            // 将两个集合拼接, 笛卡尔积
            for (String data : datas) {
                for (String city : citys) {
                    context.write(new Text(data + "," + key.toString()), new Text(city));
                }
            }
        }
    }
}
