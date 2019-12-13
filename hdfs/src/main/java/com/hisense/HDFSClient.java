package com.hisense;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS客户端
 *
 * @author zhaowenming
 */
public class HDFSClient {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1获取hdfs客户端对象
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2在hdfs上创建路径
        fs.mkdirs(new Path("/dabing"));
        fs.delete(new Path("/dabing"), true);
        // 3关闭资源
        fs.close();
        System.out.println("over");
    }
}
