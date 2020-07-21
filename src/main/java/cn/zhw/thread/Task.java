package cn.zhw.thread;

import java.io.Serializable;

public class Task extends Thread implements Serializable {
    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            System.out.println(i);
        }
    }
}
