package com.fqz.locks.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author fuqianzhong
 * @date 19/1/18
 * user CuratorFramework.InterProcessMutex
 */
public class ZkDistributeLockUseCurator {

    private static final Logger logger = LoggerFactory.getLogger(ZkDistributeLockUseCurator.class);
    private CuratorFramework curatorClient;
    private InterProcessMutex lock;
    private String path;

    public ZkDistributeLockUseCurator(String zkAddress, String path) {
        try {
            curatorClient = CuratorFrameworkFactory.builder().connectString(zkAddress)
                    .sessionTimeoutMs(2000).connectionTimeoutMs(2000)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
            curatorClient.start();
            this.path = path;
            lock = new InterProcessMutex(curatorClient, path);
        } catch (Exception e) {
            logger.error("",e);
        }
    }

    public void lock() throws Exception {
        lock.acquire();
        getChildren();
    }

    public void unlock() throws Exception {
        lock.release();
        getChildren();
    }

    public List<String> getChildren() throws Exception {
        List<String> children = curatorClient.getChildren().forPath(path);
        System.out.println(children);
        return children;
    }

    public void close(){
        if(curatorClient != null){
            curatorClient.close();
        }
    }
}
