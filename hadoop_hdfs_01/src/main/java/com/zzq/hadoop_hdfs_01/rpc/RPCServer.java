package com.zzq.hadoop_hdfs_01.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class RPCServer  implements  Bizable{

    @Override
    public String sysHi(String name ){
        return "Hi~"+name;
    }

    public static void main(String[] args)  throws  Exception{
        Server server = new RPC.Builder(new Configuration())
                .setInstance(new RPCServer())
                .setBindAddress("127.0.0.1")
                .setPort(9527)
                .setProtocol(Bizable.class)
                .build();
        server.start();
    }

}
