package com.fqz.locks.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author fuqianzhong
 * @date 19/1/18
 * 1. connect to zk server
 * 2. add EPHEMERAL_SEQUENTIAL node to root path, [/locks/key/locks-0000000001,/locks/key/locks-0000000002]
 * 3. watch first node/notify
 */
public class ZkDistributeLock {
    private static final Logger logger = LoggerFactory.getLogger(ZkDistributeLockUseCurator.class);
    private static final String ROOT_LOCK = "/locks/";

    private boolean acquireLock = false;

    private static final String CHARSET = "UTF-8";

    private static final Object lock = new Object();

    private int retries = 5;

    private int retryInterval = 3000;

    private int sessionTimeout = 30 * 1000;

    private int connectionTimeout = 15 * 1000;

    private String path;

    private String currentPath;

    private static ThreadPoolExecutor curatorEventListenerThreadPool = new ThreadPoolExecutor(100,200,60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(30),new ThreadPoolExecutor.DiscardPolicy());

    private CuratorFramework client;

    private void init() throws Exception {
        if(client == null){
            CuratorFramework client = CuratorFrameworkFactory.builder().connectString("localhost:3181,localhost:3182,localhost:3183")
                    .sessionTimeoutMs(sessionTimeout).connectionTimeoutMs(connectionTimeout)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))//最多重试3次,每次重试的时间间隔递增
                    .build();
            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    logger.info("[ZK] Connect Success : " + newState.name().toLowerCase());
                }
            });
            client.getCuratorListenable().addListener(new CuratorEventListener(), curatorEventListenerThreadPool);
            client.start();
            boolean isConnected = client.getZookeeperClient().blockUntilConnectedOrTimedOut();
            CuratorFramework oldClient = this.client;
            this.client = client;
            close(oldClient);
            if(!isConnected) {
                throw new Exception("[ZK] connection timeout");
            }
        }
    }

    //1. connect to zk server
    public ZkDistributeLock(String path) {
        try {
            init();
            this.path = path;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("<<<<<< error init zk >>>>>>", e);
        }
    }

    public void lock() throws Exception {

        String currentPath = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(  parsePath() + "/locks-", "".getBytes(CHARSET));
        System.out.println(client.getChildren().forPath( parsePath() ));

        this.currentPath = currentPath;

        while (true) {
            String firstNode = getFirstNode();

            if (currentPath != null && firstNode != null && currentPath.equals(firstNode)) {
                break;
            }

            synchronized (lock) {

                //监听path最小的节点
                client.getChildren().watched().forPath(firstNode);

                lock.wait();

                System.out.println(Thread.currentThread().getName()+" continue to apply lock ");

            }
        }

    }

    public void unlock() throws Exception {
        if(currentPath == null){
            return;
        }
        client.delete().forPath(currentPath);

    }

    private String parsePath(){
        return ROOT_LOCK + path;
    }

    private String getFirstNode() throws Exception {
        List<String> children = client.getChildren().watched().forPath(parsePath());
        if (children != null && children.size() > 0) {
            Collections.sort(children, new Comparator<String>() {
                @Override
                public int compare(String path1, String path2) {
                    return path1.compareTo(path2);
                }
            });

            return  parsePath() +"/" + children.get(0);

        }
        return null;
    }

    private class CuratorEventListener implements CuratorListener {

        @Override
        public void eventReceived(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
            WatchedEvent event = (curatorEvent == null ? null : curatorEvent.getWatchedEvent());
            if (event == null ||
                    (event.getType() != Watcher.Event.EventType.NodeCreated && event.getType() != Watcher.Event.EventType.NodeDataChanged
                            && event.getType() != Watcher.Event.EventType.NodeDeleted && event.getType() != Watcher.Event.EventType.NodeChildrenChanged)) {
                return;
            }

            if(event.getType() == Watcher.Event.EventType.NodeDeleted){
                synchronized (lock){
                    System.out.println("delete  " + curatorEvent.getPath() + ", notify others");
                    lock.notifyAll();
                }
            }
        }
    }
    private void close(CuratorFramework client) {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
            }
        }
    }
}
