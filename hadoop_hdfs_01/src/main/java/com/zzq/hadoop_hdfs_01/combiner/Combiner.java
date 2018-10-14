package com.zzq.hadoop_hdfs_01.combiner;

import com.zzq.hadoop_hdfs_01.mapreduce.MyCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.Files;

/**
 * 在mapper 执行之后执行一次， 相当于一个reducer
 */
public class Combiner {

    public static  class  CountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        private Text k =  new Text();
        private LongWritable v = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fileds = value.toString().split(" ");
            for (String filed:
                 fileds) {
                k.set(filed);
                v.set(1);
                context.write(k,v);
            }
        }
    }


    public static class  CountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        private LongWritable v = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0 ;
            for (LongWritable value:
                 values) {
               sum +=value.get();
            }
            v.set(sum);
            context.write(key,v);
        }
    }

    public static void main(String[] args)  throws  Exception{
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Combiner.class);

        job.setMapperClass(CountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\27660\\Desktop\\u.txt"));

        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\27660\\Desktop\\info"));

        job.setCombinerClass(CountReducer.class);

        job.waitForCompletion(true);
    }

}
