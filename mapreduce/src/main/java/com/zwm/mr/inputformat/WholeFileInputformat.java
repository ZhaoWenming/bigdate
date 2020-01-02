package com.zwm.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * [注意]该纹在所属的inputformat项目并不能运行，因为没有写完
 *
 * @author zhaowenming
 */
public class WholeFileInputformat extends FileInputFormat<Text, BytesWritable> {
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeFileInputReader recordReader =  new WholeFileInputReader();
        recordReader.initialize(split, context);
        return null;
    }
}
