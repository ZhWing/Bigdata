package cn.zhw.thread;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class NodeManager {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ServerSocket serverSocket = new ServerSocket(8888);
        System.out.println("NodeManager 已启动");

        // 等待请求
        Socket accept = serverSocket.accept();

        // 获取输入流
        InputStream inputStream = accept.getInputStream();

        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

        // 接收发送来的线程对象
        Task task = (Task) objectInputStream.readObject();

        task.start();

        objectInputStream.close();
        accept.close();
    }
}
