package cn.zhw.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfsUtil {
    public static void main(String[] args) throws URISyntaxException, IOException {
//        System.setProperty("HADOOP_USER_NAME","root");
        // 首先获取 hdfs 集群访问地址，nameNode 的地址
        URI uri = new URI("hdfs://192.168.100.132:9000");
        // 创建一个配置文件对象
        Configuration conf = new Configuration();
        // 创建 hdfs 文件系统对象
        FileSystem fs = FileSystem.get(uri, conf);

        // 创建一个文件夹
        fs.create(new Path("/hdfs001"));

        // 本地文件上传 hdfs
        fs.copyFromLocalFile(new Path("data/word.txt"), new Path("/data"));

        // 流上传
        FileInputStream in = new FileInputStream("data/word.txt");
        FSDataOutputStream out = fs.create(new Path("/data/001"));
        byte[] b = new byte[1024 * 1024];
        int read = 0;
        while ((read = in.read(b)) > 0) {
            out.write(b, 0, read);
        }

        // 追加内容
        FileInputStream fis = new FileInputStream("data/word.txt");
        FSDataOutputStream append = fs.append(new Path("/data/1.txt"));

        // append.write("678910".getBytes());
        byte[] b1 = new byte[1024 * 1024];
        int flag = 0;
        while ((flag = fis.read(b1)) > 0) {
            append.write(b1, 0, flag);
        }

        // 将 hdfs 文件下载到本地
        FSDataInputStream in1 = fs.open(new Path("/data/001"));
        FileOutputStream out1 = new FileOutputStream("data/words.txt");
        IOUtils.copyBytes(in1, out1, conf);

        // 删除一个文件夹
        fs.delete(new Path("/hdfs001"));

        // 查看路径信息
        FileStatus[] listStatus = fs.listStatus(new Path("/data"));
        for (FileStatus status : listStatus) {
            if (status.isFile())
                System.out.println(status.getPath().getName());
        }
    }
}
