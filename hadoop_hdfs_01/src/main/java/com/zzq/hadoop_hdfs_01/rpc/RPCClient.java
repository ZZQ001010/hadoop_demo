package com.zzq.hadoop_hdfs_01.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class RPCClient {
    public static void main(String[] args) throws  Exception {
        Bizable proxy = RPC.getProxy(Bizable.class, 1,
                new InetSocketAddress("192.168.186.103", 9527), new Configuration());
        String tomcat = proxy.sysHi("tomcat");
        System.out.println(tomcat);
        RPC.stopProxy(proxy);
    }
}
