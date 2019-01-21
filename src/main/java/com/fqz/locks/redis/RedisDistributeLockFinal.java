package com.fqz.locks.redis;

/**
 * @author fuqianzhong
 * @date 19/1/18
 * 针对客户端时间有可能不同步和线程可以任意释放锁的问题,
 * 1. 在redis服务端,添加key的超时时间,需要用到原子命令,set(key,value,nx,px,time)
 * 2. 每个线程对应唯一的id,申请到锁后,保存在value里,释放锁时,只有当前id对应的线程可以释放
 * 3. 客户端如果释放锁失败(错误的释放程序,服务重启等),需要等锁超时后,才可以重新获取
 *
 * 要求:redis>=2.6.0
 */
public class RedisDistributeLockFinal {

    private static final long EXPIRE_TIME = 1*15*1000;

    //block current thread
    public static void lock(String key,String requestId) throws InterruptedException {

        while (true){
            if(setKey(key,requestId)){
                break;
            }
            Thread.sleep(1000);
        }
    }

    public static void unlock(String key, String requestId){
        if(key == null || key.trim().length() == 0 || requestId == null || requestId.trim().length() == 0){
            return ;
        }
        //拿到值再比较会有并发问题,使用lua脚本原子性操作
//        String currentRequestId = JedisUtils.getString(key);
//        if(currentRequestId != null && requestId.equals(currentRequestId)){
//            JedisUtils.deleteKey(key);
//        }

        JedisUtils.delKeyWhenValueEquals(key,requestId);

    }

    //return immediately
    private static boolean setKey(String key, String requestId){

        if(key == null || key.trim().length() == 0 || requestId == null || requestId.trim().length() == 0){
            return false;
        }
        String result = JedisUtils.setNxExpired(key, requestId, EXPIRE_TIME);
        if(result != null && result.equals(JedisUtils.SUCCESS)){
            return true;
        }
        return false;

    }

}
