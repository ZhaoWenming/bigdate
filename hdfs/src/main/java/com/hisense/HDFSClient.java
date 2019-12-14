package com.hisense;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS客户端
 *
 * @author zhaowenming
 */
@Slf4j
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
        log.debug("over");
    }

    // 1文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2执行api操纵
        fs.copyFromLocalFile(new Path("e:/helloword.txt"), new Path("/ml.txt"));
        // 3关闭资源
        fs.close();
        log.debug("upload success!");
    }

    // 2文件下载
    @Test
    public void testCopyToLocalFiel() throws URISyntaxException, IOException, InterruptedException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2执行下载操作
        System.setProperty("hadoop.home.dir", "/");
        fs.copyToLocalFile(false, new Path("/ml.txt"), new Path("e:/banhua1.txt"), true);
        // 3关闭资源
        fs.close();
        log.debug("download success!");
    }

    // 3文件删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2文件删除
        boolean delete = fs.delete(new Path("/user"), true);
        // 2关闭资源
        fs.close();
        log.debug("delete" + delete);
    }
    // 4修改文件名称
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2执行更名操作
        fs.rename(new Path("/ml.txt"), new Path("/updata.txt"));
        // 2关闭资源
        fs.close();
        log.debug("rename success");
    }

    // 5查看文件详情
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            //查看文件名称、权限、长度、块信息
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-------分割线----------------");
        }
        // 3关闭资源
        fs.close();
        log.debug("list file success");
    }

    // 6判断是文件还是文件夹
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2判断操作
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                // 文件
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                // 文件夹
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 2关闭资源
        fs.close();
        log.debug("rename success");
    }
}
