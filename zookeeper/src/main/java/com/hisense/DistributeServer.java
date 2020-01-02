package com.hisense;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer server = new DistributeServer();

        // 1连接zookeeper集群
        server.getConnect();
        // 2注册节点
        server.regist(args[0]);
        // 3业务逻辑
        server.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void regist(String hostname) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online ");
    }

    private String connectString = "centos102:2181,centos103:2181,centos104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }
}
