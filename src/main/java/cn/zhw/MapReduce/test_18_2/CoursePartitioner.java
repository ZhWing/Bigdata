package cn.zhw.MapReduce.test_18_2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CoursePartitioner extends Partitioner<CourseBean, NullWritable> {
    @Override
    public int getPartition(CourseBean key, NullWritable value, int numPartitions) {
        if ("algorithm".equals(key.getCourse())) {
            return 0;
        } else if ("computer".equals(key.getCourse())) {
            return 1;
        } else if ("english".equals(key.getCourse())) {
            return 2;
        } else {
            return 3;
        }
    }
}
