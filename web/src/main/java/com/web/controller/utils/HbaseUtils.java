package com.web.controller.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HbaseUtils {

    public Configuration getHbaseConf(String hbaseQuorum, String zookeeperPort) throws Exception {
        Configuration hbaseConf = null;
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", hbaseQuorum);
        hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        return hbaseConf;

    }

    public Table getTable(Configuration hbaseConf, String tableName){
        Table table = null;
        try{
            Connection conn = ConnectionFactory.createConnection(hbaseConf);
            table = conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

}
