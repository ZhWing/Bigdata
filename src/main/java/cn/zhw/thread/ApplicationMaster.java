package cn.zhw.thread;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ApplicationMaster {
    public static void main(String[] args) throws IOException {

        // 创建线程对对象
        Task task = new Task();
        Socket socket = new Socket("localhost", 8888);

        OutputStream outputStream = socket.getOutputStream();

        // 发送对象 使用对象流
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        System.out.println("调度 task 中");
        // 发送 task 到 nodemanager 中
        objectOutputStream.writeObject(task);
        objectOutputStream.flush();

        // 关闭资源
        objectOutputStream.close();
        socket.close();
    }
}
