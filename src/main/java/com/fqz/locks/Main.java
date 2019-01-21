package com.fqz.locks;

/**
 * @author fuqianzhong
 * @date 19/1/17
 */
public class Main {
    public static void main(String[] args) {
        //redis test

        Thread thread = new Thread(new LockRunner());
        thread.setName("t1-1");
        Thread thread2 = new Thread(new LockRunner());
        thread2.setName("t1-2");
        thread.start();
        thread2.start();

    }
}
