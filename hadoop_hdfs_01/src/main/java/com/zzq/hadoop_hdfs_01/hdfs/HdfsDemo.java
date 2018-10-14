package com.zzq.hadoop_hdfs_01.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HdfsDemo {

    private FileSystem fs = null;

    @Before
    public void init() throws  Exception{
         fs = FileSystem.get(new URI("hdfs://192.168.186.149:9000"),new Configuration(),"root");
    }

    @Test
    public void delete() throws  Exception{
        fs.delete(new Path("/dir"),true);
    }

    @Test
    public void download()  throws  Exception{
        FSDataInputStream in = fs.open(new Path("/in.log"));
        FileOutputStream out = new FileOutputStream("C:\\Users\\27660\\Desktop\\in.log");
        org.apache.hadoop.io.IOUtils.copyBytes(in,out,4096,true);
    }

    @Test
    public void upload() throws  Exception{
        FSDataOutputStream out = fs.create(new Path("/1.pdf"));
        FileInputStream in  = new FileInputStream(new File("C:\\Users\\27660\\Desktop\\coreJava_面试题.pdf"));
        org.apache.hadoop.io.IOUtils.copyBytes(in,out,4096,true);
    }


    @Test
    public void downloadQ() throws  Exception{
       fs.copyToLocalFile(false,new Path("/1.pdf"),new Path("C:\\Users\\27660\\Desktop\\1.txt"),true);
    }

    @Test
    public void uploadQ() throws  Exception{
        //delsrc 是否删除源文件
        fs.copyFromLocalFile(true,new Path("C:\\Users\\27660\\Desktop\\1.txt"),new Path("/1.txt"));
    }


    @Test
    public void mkdir() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/dir"));
        System.out.println(mkdirs);
    }


    /**
     * 此方法是可以的
     * @throws Exception
     */
    @Test
    public void uploadDir() throws  Exception{
        fs.copyFromLocalFile(true,new Path("C:\\Users\\27660\\Desktop\\a\\"),new Path("/a/"));
    }

    @Test
    public void downloadDir() throws  Exception{
        fs.copyToLocalFile(false,new Path("/a/"),new Path("C:\\Users\\27660\\Desktop\\a\\"),true);
    }




}
