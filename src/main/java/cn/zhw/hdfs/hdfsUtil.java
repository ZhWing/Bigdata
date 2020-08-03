package cn.zhw.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfsUtil {
    public static void main(String[] args) throws URISyntaxException, IOException {
//        System.setProperty("HADOOP_USER_NAME","root");
        // 首先获取 hdfs 集群访问地址，nameNode 的地址
        URI uri = new URI("hdfs://192.168.247.137:9000");
        // 创建一个配置文件对象
        Configuration conf = new Configuration();
        // 创建 hdfs 文件系统对象
        FileSystem fs = FileSystem.get(uri, conf);

        // 创建一个文件夹
//         fs.create(new Path("/hdfs001"));

        // 删除一个文件夹
//         fs.delete(new Path("/hdfs001"));

        // 查看路径信息
        FileStatus[] listStatus = fs.listStatus(new Path("/data"));
        for (FileStatus status : listStatus) {
            if (status.isFile())
                System.out.println(status.getPath().getName());
        }
    }
}
