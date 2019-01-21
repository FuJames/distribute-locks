package com.fqz.locks.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by fuqianzhong on 17/2/17.
 * Curator是操作Zookeeper的框架,它简化了ZooKeeper的操作,增加了很多使用ZooKeeper开发的特性，可以处理ZooKeeper集群复杂的连接管理和重试机制,
 * 使用线程池来监听事件
 */
public class CuratorClient {
    private static final String CHARSET = "UTF-8";

    private CuratorFramework client;

    private int retries = 5;

    private int retryInterval = 3000;

    private int sessionTimeout = 30 * 1000;

    private int connectionTimeout = 15 * 1000;

    private static ThreadPoolExecutor curatorEventListenerThreadPool = new ThreadPoolExecutor(100,200,60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(30),new ThreadPoolExecutor.DiscardPolicy());

    private String address;

    public CuratorClient(String zkAddress) throws Exception {
        this.address = zkAddress;
        newCuratorClient();
    }

    private boolean newCuratorClient() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(address)
                .sessionTimeoutMs(sessionTimeout).connectionTimeoutMs(connectionTimeout)
                .retryPolicy(new MyRetryPolicy(retries, retryInterval)).build();
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println("[ZK] Connect Success : " + newState.name().toLowerCase());
            }
        });
        client.getCuratorListenable().addListener(new CuratorEventListener(this), curatorEventListenerThreadPool);
        client.start();
        boolean isConnected = client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        CuratorFramework oldClient = this.client;
        this.client = client;
        close(oldClient);
        if(!isConnected) {
            throw new Exception("[ZK] connection timeout");
        }
        return isConnected;
    }

    private static class MyRetryPolicy extends RetryNTimes {
        private final int sleepMsBetweenRetries;
        private int retryCount;

        public MyRetryPolicy(int n, int sleepMsBetweenRetries) {
            super(n, sleepMsBetweenRetries);
            this.sleepMsBetweenRetries = sleepMsBetweenRetries;
        }

        @Override
        protected int getSleepTimeMs(int retryCount, long elapsedTimeMs) {
            this.retryCount = retryCount;
            return sleepMsBetweenRetries;
        }

        public int getRetryCount() {
            return retryCount;
        }
    }

    public String getData(String path) throws Exception {
        return getData(path, true);
    }

    public String getWithNodeExistsEx(String path, Stat stat) throws Exception {
        if (exists(path, false)) {
            byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
            String value = new String(bytes, CHARSET);
            return value;
        } else {
            throw new KeeperException.NodeExistsException("node " + path + " does not exist");
        }
    }

    public String getData(String path, Stat stat) throws Exception {
        if (exists(path, false)) {
            byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
            String value = new String(bytes, CHARSET);
            return value;
        } else {
            return null;
        }
    }

    public String getData(String path, boolean watch) throws Exception {
        if (exists(path, watch)) {
            byte[] bytes = client.getData().watched().forPath(path);
            String value = new String(bytes, CHARSET);
            return value;
        } else {
            return null;
        }
    }
    public String getData(String path, CuratorWatcher watcher) throws Exception {
        if (exists(path, true)) {
            byte[] bytes = client.getData().usingWatcher(watcher).forPath(path);
            String value = new String(bytes, CHARSET);
            return value;
        } else {
            return null;
        }
    }

    public void setData(String path, Object value, int version) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        if (exists(path, false)) {
            client.setData().withVersion(version).forPath(path, bytes);
        } else {
            client.create().creatingParentsIfNeeded().forPath(path, bytes);
        }
    }

    public void setData(String path, Object value) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        if (exists(path, false)) {
            client.setData().forPath(path, bytes);
        } else {
            client.create().creatingParentsIfNeeded().forPath(path, bytes);
        }
    }

    public void createIsAbsent(String path) throws Exception {
        if(!exists(path,false)){
            create(path, null);
        }
    }

    public void create(String path) throws Exception {
        create(path, null);
    }

    public void create(String path, Object value, int version) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        client.create().creatingParentsIfNeeded().withProtection().forPath(path, bytes);
    }

    public void create(String path, Object value) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        //默认是持久化节点
        client.create().creatingParentsIfNeeded().forPath(path, bytes);
    }

    public void createEphemeral(String path, String value) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
    }

    public String create(String path, String value, CreateMode nodeType) throws Exception {
        byte[] bytes = (value == null ? new byte[0] : value.toString().getBytes(CHARSET));
        return client.create().creatingParentsIfNeeded().withMode(nodeType).forPath(path, bytes);
    }

    public void createEphemeral(String path) throws Exception {
        createEphemeral(path, null);
    }


    public boolean exists(String path) throws Exception {
        Stat stat = client.checkExists().watched().forPath(path);
        return stat != null;
    }

    public boolean exists(String path, boolean watch) throws Exception {
        Stat stat = watch ? client.checkExists().watched().forPath(path) : client.checkExists().forPath(path);
        return stat != null;
    }

    public List<String> getChildren(String path) throws Exception {
        return getChildren(path, true);
    }

    public List<String> getChildren(String path, boolean watch) throws Exception {
        try {
            List<String> children = watch ? client.getChildren().watched().forPath(path)
                    : client.getChildren().forPath(path);
            return children;
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<String>();
        }
    }

    public void deleteIfExists(String path) throws Exception {
        if (exists(path, false)) {
            delete(path);
        }
    }

    public void delete(String path) throws Exception {
        client.delete().forPath(path);
    }

    public void watch(String path) throws Exception {
        client.checkExists().watched().forPath(path);
    }

    public void watchChildren(String path) throws Exception {
        if (exists(path)) {
            client.getChildren().watched().forPath(path);
        }
    }

    public void close() {
        this.close(this.client);
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
