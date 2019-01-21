package com.fqz.locks;

import com.fqz.locks.zk.ZkDistributeLock;

import java.util.UUID;

/**
 * @author fuqianzhong
 * @date 19/1/18
 */
public class LockRunner implements Runnable{
    private static int i = 0;
    @Override
    public void run() {
        String requestId = UUID.randomUUID().toString();
        String tName = Thread.currentThread().getName();
        System.out.println(tName + " wait lock >>>");
//        ZkDistributeLockUseCurator lock = new ZkDistributeLockUseCurator("localhost:3181,localhost:3182,localhost:3183","/locks/test");
        ZkDistributeLock lock = new ZkDistributeLock("key");
        try {
//            RedisDistributeLockFinal.lock("key", requestId);

            lock.lock();
            System.out.println(tName + " acquire lock >>>");
            int step = 5;
            while (step > 0){
                System.out.println(tName+":"+i++);
                step --;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
//            RedisDistributeLockFinal.unlock("key",requestId);
            try {
                lock.unlock();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println(tName + " release lock >>>");

    }
}
