package cn.zhw.MapReduce.test_19_2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class CoursePartitioner extends Partitioner<Student, NullWritable>{
	@Override
	public int getPartition(Student key, NullWritable value, int numPartitions) {
		if ("algorithm".equals(key.getCourse())){
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
