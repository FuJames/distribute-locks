package com.fqz.locks;

/**
 * @author fuqianzhong
 * @date 19/1/18
 */
public class Main2 {
    public static void main(String[] args) {
        Thread thread = new Thread(new LockRunner());
        thread.setName("t2-1");
        Thread thread2 = new Thread(new LockRunner());
        thread2.setName("t2-2");
        thread.start();
        thread2.start();

    }
}
