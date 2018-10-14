package com.zzq.hadoop_hdfs_01.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import sun.rmi.runtime.Log;
import sun.swing.plaf.synth.DefaultSynthStyle;

import java.beans.beancontext.BeanContext;
import java.io.IOException;

/**
 *
 */
public class Sum {

    public static  class  MyMapper extends Mapper<LongWritable,Text,Text,InfoBean>{

        private InfoBean infoBean = new InfoBean();
        private Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fieds = line.split("\t");
            String account =fieds[0];
            double income =Double.parseDouble(fieds[1]);
            double expenses =Double.parseDouble(fieds[2]);
            k.set(account);
            infoBean.set(account,income,expenses);
            context.write(k,infoBean);
        }
    }

    public static  class  MyReduce extends Reducer<Text,InfoBean,Text,InfoBean>{

        private InfoBean bean = new InfoBean() ;
        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            double sum_income = 0 ;
            double sum_expenses =0 ;
            for (InfoBean bean:
                 values) {
                sum_income+=bean.getIncome();
                sum_expenses+=bean.getExpenses();
            }
            bean.set("",sum_income,sum_expenses);
            context.write(key,bean);
        }
    }


    public static void main(String[] args)  throws  Exception{
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Sum.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\27660\\Desktop\\trade_info.txt"));

        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\27660\\Desktop\\info"));

        job.waitForCompletion(true);
    }

}
