package com.fqz.locks.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @author fuqianzhong
 * @date 18/5/18
 */
public class CuratorEventListener implements CuratorListener {
    private CuratorClient client;

    public CuratorEventListener(CuratorClient curatorClient){
        this.client = curatorClient;
    }

    @Override
    public void eventReceived(CuratorFramework client, CuratorEvent curatorEvent) throws Exception {
        WatchedEvent event = (curatorEvent == null ? null : curatorEvent.getWatchedEvent());
        if (event == null ||
                (event.getType() != Watcher.Event.EventType.NodeCreated && event.getType() != Watcher.Event.EventType.NodeDataChanged
                && event.getType() != Watcher.Event.EventType.NodeDeleted && event.getType() != Watcher.Event.EventType.NodeChildrenChanged)) {
            return;
        }
        String path = event.getPath();

    }
}

