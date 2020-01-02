package com.hisense;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DistributeClient {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeClient client = new DistributeClient();

        // 1获取zookeeper集群连接
        client.getConnect();
        // 2注册监听
        client.getChilden();
        // 3业务逻辑处理
        client.bussiness();
    }

    private void bussiness() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChilden() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/servers", true);

        // 存储服务器节点主机名称集合
        ArrayList<String> hosts = new ArrayList<>();

        for (String child : children) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }

        // 将所有在线主机名称打印到控制台
        System.out.println(hosts);
    }

    private String connectString = "centos102:2181,centos103:2181,centos104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getChilden();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
