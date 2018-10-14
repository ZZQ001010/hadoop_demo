package com.zzq.hadoop_hdfs_01.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

//in k2 v2   out k v
public class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count = 0 ;
        for (LongWritable l : values) {
            count+=l.get();
        }

        context.write(key,new LongWritable(count));
    }
}
