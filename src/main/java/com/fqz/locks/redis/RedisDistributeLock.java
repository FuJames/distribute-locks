package com.fqz.locks.redis;

/**
 * @author fuqianzhong
 * @date 19/1/17
 * a. setnx 原子命令,并发情况下只有一个线程可以设置成功
 *
 * b. 释放锁: 正常情况下,释放锁通过删除key来完成,但有些情况下,比如jvm崩溃、服务重启、服务器异常、硬件异常,导致正常释放锁的程序没有执行。
 *           引入超时机制来解决问题,线程在使用setnx请求锁时,如果申请成功,setnx将值设置为当前时间,如果申请失败,则获取key对应的时间(这里用到getset原子命令,获取到原有值并设置新值,因为并发情况下多个线程拿到的时间有可能相同,所以需要设置新值),与超时时间做比较,如果没有超时,则获取到锁,否则,获取锁失败;
 *
 * 弊端:
 *
 * 1. 超时时间由客户端设定,分布式环境下,需要各个客户端的时间同步,如果是跨时区的情况,不建议使用
 *
 * 2. 任意线程均可以释放锁,A申请到锁,A任务执行很久,锁超时,B申请到锁,A在B之前执行完成,A释放锁,此时B的锁会被释放
 *
 */
public class RedisDistributeLock {
    private static final long EXPIRE_TIME = 1*60*1000;

    //block current thread
    public static void lock(String key) throws InterruptedException {

        while (true){
            if(setKey(key)){
                break;
            }
            Thread.sleep(1000);
        }
    }

    private static boolean setKey(String key){

        if(key == null || key.trim().length() == 0){
            return false;
        }
        Long currentTime = System.currentTimeMillis();
        String expiredTime = String.valueOf(currentTime + EXPIRE_TIME);
        Long row = JedisUtils.setNx(key, expiredTime);
        //setnx success
        if(row != null && row == 1){
            return true;
        }
        String oldExpiredTime = JedisUtils.getString(key);

        //检查key是否过期
        if(oldExpiredTime != null && Long.valueOf(oldExpiredTime).compareTo(currentTime) >= 0){
            //同一时刻只有一个线程可以获取到锁
            String oldExpiredTimeAfterSet = JedisUtils.getSet(key, expiredTime);

            if(oldExpiredTimeAfterSet != null && oldExpiredTimeAfterSet.equals(oldExpiredTime)) {
                return true;
            }
        }
        return false;
    }

    public static void unlock(String key){
        if(key == null || key.trim().length() == 0){
            return ;
        }

        JedisUtils.deleteKey(key);
    }
}
