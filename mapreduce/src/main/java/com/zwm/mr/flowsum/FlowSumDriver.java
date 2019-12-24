package com.zwm.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSumDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"e:/input/inputflow", "e:/output2"};

        // 1获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2设置jar路径
        job.setJarByClass(FlowSumDriver.class);
        // 3关联mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        // 4设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 5设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /**
         * 6,7两个步骤没有进行调试，如果调试不通过，将7，8注释掉即可
         */
        // 6果不设置InputFormat，它默认用的是TextInputFormat.class
        //job.setInputFormatClass(CombineTextInputFormat.class);
        // 7虚拟存储切片最大值设置4m
        //CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

        // 8设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 9提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
