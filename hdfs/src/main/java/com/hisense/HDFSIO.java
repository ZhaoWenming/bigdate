package com.hisense;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSIO {

    // 1将e盘上的banhau.txt文件上传到HDFS根目录
    @Test
    public void putFileToHTFS() throws IOException, URISyntaxException, InterruptedException {
        // 1获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2获取输入流
        FileInputStream fis = new FileInputStream(new File("e:/README.txt"));
        // 3获取输出流
        FSDataOutputStream fos = fs.create(new Path("/README.txt"));
        // 4流的对拷
        IOUtils.copyBytes(fis, fos, conf);
        // 5关闭资
        IOUtils.closeStreams(fis, fos);
        fs.close();
    }

    // 2从HDFS上下载update.txt文件到本地e盘上
    @Test
    public void getFileFromHTFS() throws IOException, URISyntaxException, InterruptedException {
        // 1获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2获取输入流
        FSDataInputStream fis = fs.open(new Path("/updata.txt"));
        // 3获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/updata.txt"));
        // 4流的对拷
        IOUtils.copyBytes(fis, fos, conf);
        // 5关闭资
        IOUtils.closeStreams(fis, fos);
        fs.close();
    }

    // 3分块读取HDFS上的大文件，比如根目录下的/hadoop-2.10.0.tar.gz

    // 下载第一块
    @Test
    public void readFileSeek1() throws IOException, URISyntaxException, InterruptedException {
        // 1获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos102:9000"), conf, "zwm");
        // 2获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.10.0.tar.gz"));
        // 3获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.10.0.tar.gz.part1"));
        // 4流的对拷（只拷128m）
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            
        }
        // 5关闭资
        IOUtils.closeStreams(fis, fos);
        fs.close();
    }

}
