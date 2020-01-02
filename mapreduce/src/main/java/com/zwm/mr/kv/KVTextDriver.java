package com.zwm.mr.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

         args = new String[]{"e:/input/kv", "e:/output"};

        Configuration conf = new Configuration();

        // [重要]设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        // 1获取job对象
        Job job = Job.getInstance(conf);
        // 2设置jar存放位置
        job.setJarByClass(KVTextDriver.class);

        // 3关联Map和Reduce类
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        // 4设置Map阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5设置最终输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // [重要]设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 6设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7提交job
        // job.submit();
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
