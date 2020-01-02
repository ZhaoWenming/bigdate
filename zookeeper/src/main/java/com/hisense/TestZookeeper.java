package com.hisense;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZookeeper {
    private String connectString = "centos102:2181,centos103:2181,centos104:2181";
    private int sessionTimeout = 2000;
    public ZooKeeper zkClient;

    // 创建zookeeper客户端
    @Before
    public void init() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            // 2获取子节点并监听节点的变化
            public void process(WatchedEvent event) {
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (String child : children) {
                    System.out.println(child);
                }
            }
        });
    }

    // 1创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String node = zkClient.create("/zwm1", "dahaigezuishuai".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(node);
        Thread.sleep(Long.MAX_VALUE);

    }

    // 2获取子节点并监听节点的变化,实质上是在process函数中注册的监听，执行的也是process中的代码
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    // 3判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat banzhang = zkClient.exists("/banzhang", false);
        System.out.println(banzhang);
    }
}
