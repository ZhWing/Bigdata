package cn.zhw.MapReduce.test_18_1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * 2018 年大数据应用大赛2
 * 编写自动脚本，在0-100的范围内生成随机整数，共生成5万个数字，以英文逗号进行分隔
 */
public class MakeWords {
    public static void main(String[] args) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("data/words.txt"));
        // 产生随机数据，写入文件
        Random random = new Random();

        for (int i = 0; i < 50000; i++) {
            // 产生0-100之间随机数
            int randint = (int) Math.floor((random.nextDouble() * 100.0));
            if (i == 0)
                // 写入一个随机数
                bw.write(String.valueOf(randint));
            else
                bw.write("," + randint);
            // bw.newLine(); //新的一行
        }
        bw.close();
    }
}
