package com.zzq.hadoop_hdfs_01.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.sound.sampled.Line;
import java.io.IOException;

public class Sort {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Sort.class);

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\27660\\Desktop\\trade_info.txt"));

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\27660\\Desktop\\info"));

        job.waitForCompletion(true);

    }

    public static class SortMapper extends Mapper<LongWritable, Text, Text, InfoBean>{

        private InfoBean v = new InfoBean();
        private Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String account = fields[0];
            double income = Double.parseDouble(fields[1]);
            double expenses = Double.parseDouble(fields[2]);
            k.set(account);
            v.set(account, income, expenses);
            context.write(k, v);
        }

    }


    public static class SortReducer extends Reducer<Text, InfoBean, Text, InfoBean>{
        private InfoBean v = new InfoBean() ;

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context)
                    throws IOException, InterruptedException {
            double sum_in = 0 ;
            double sum_out = 0 ;
            for (InfoBean bean:
                 values) {
                sum_in+=bean.getIncome();
                sum_out+=bean.getExpenses();
            }
            v.set("",sum_in,sum_out);
            context.write(key,v);
        }
    }
}
