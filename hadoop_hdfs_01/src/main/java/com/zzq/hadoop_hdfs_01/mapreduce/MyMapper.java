package com.zzq.hadoop_hdfs_01.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //文本内容
        String line = value.toString();
        //业务逻辑
        String[] words = line.split("\t");
        for (String tmp: words
             ) {
            //放入上下文中
            context.write(new Text(tmp),new LongWritable(1));
        }
    }
}
