package com.zwm.mr.inputformat;

import org.apache.commons.compress.archivers.zip.AbstractUnicodeExtraField;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileInputReader extends RecordReader<Text, BytesWritable> {

    FileSplit split;
    Configuration configuration;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean isProgress = true;

    /**
     * Called once at initialization.
     *
     * @param split   the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 初始化
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    /**
     * Read the next key, value pair.
     *
     * @return true if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 核心业务逻辑处理

        if (isProgress) {
            byte[] buf = new byte[(int) split.getLength()];
            // 1获取fs对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(configuration);

            // 2获取输入流
            FSDataInputStream fis = fs.open(path);

            // 3拷贝
            IOUtils.readFully(fis, buf, 0, buf.length);

            // 4封装v
            v.set(buf, 0, buf.length);

            // 5封装k
            k.set(path.toString());

            // 6关闭资源
            IOUtils.closeStream(fis);

            isProgress = false;

            return true;
        }
        return false;
    }

    /**
     * Get the current key
     *
     * @return the current key or null if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    /**
     * Get the current value.
     *
     * @return the object that was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close() throws IOException {

    }
}
