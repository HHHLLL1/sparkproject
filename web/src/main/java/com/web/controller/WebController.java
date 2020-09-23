package com.web.controller;


import com.web.controller.utils.*;
import com.web.controller.utils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static java.util.Arrays.asList;


@Controller
public class WebController {

    String hbaseQuorum = "hadoop10,hadoop11,hadoop12,hadoop13";
    String zookeeperPort = "2181";
    String tableName = "tmdb";
    String tmdb_popularity = "tmdb_popularity";
    String tmdb_runtime = "tmdb_runtime";



    Configuration hbaseConf = new HbaseUtils().getHbaseConf(hbaseQuorum, zookeeperPort);

    public WebController() throws Exception {
    }

    @RequestMapping(value = {"", "/index"})
    public String index(Model model) {
//		model.addAttribute("msg", "hello world");
        return "index";
    }

    @RequestMapping(value = {"/demo"})
    @ResponseBody
    public List<Product> myProject() {

        String[] data = {"羊毛衫","雪纺衫","裤子","高跟鞋","袜子"};
        long[] nums = {20, 36, 10, 10, 20};

        ArrayList<Product> productArr = new ArrayList<Product>();

        for (int i=0; i<data.length; i++){
            Product p = new Product();
            p.setProductName(data[i]);
            p.setNums(nums[i]);
            productArr.add(p);
        }

        return productArr;
    }


    @RequestMapping(value = {"/popularity"})
    @ResponseBody
    public List<Product> Popularity() throws IOException {

        Table table = new HbaseUtils().getTable(hbaseConf, tmdb_popularity);
        Scan scan = new Scan();

        ResultScanner resultScanner = table.getScanner(scan);

        ArrayList<Product> productArr = new ArrayList<Product>();

        long[] popularity = new long[10];
        String[] original_title = new String[10];

        int oindex = 0;
        int pindex = 0;

        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells) {
//                //得到rowkey
//                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
//                //得到列族
//                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
//                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
//                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));

                String col = Bytes.toString(CellUtil.cloneQualifier(cell));

                if (col.equals("original_title")) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    original_title[oindex++] = value.split(":")[0];
                } else if (col.equals("popularity")) {
                    Float value = Bytes.toFloat(CellUtil.cloneValue(cell));
                    popularity[pindex++] = value.longValue();
                }
            }
        }

        System.out.println(Arrays.toString(original_title));
        System.out.println(Arrays.toString(popularity));

        for (int i=0; i<original_title.length; i++){
            Product p = new Product();
            p.setProductName(original_title[i]);
            p.setNums(popularity[i]);
            productArr.add(p);
        }

        return productArr;
    }

    @RequestMapping(value = {"/runtime"})
    @ResponseBody
    public List<Product> Runtime() throws IOException {
        Table table = new HbaseUtils().getTable(hbaseConf, tmdb_runtime);
        Scan scan = new Scan();

        ResultScanner resultScanner = table.getScanner(scan);

        ArrayList<Product> productArr = new ArrayList<Product>();

        long[] count = new long[16];
        String[] runtime = new String[16];

        int rindex = 0;
        int cindex = 0;

        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();

            for(Cell cell : cells) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (col.equals("runtime")) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    runtime[rindex++] = value;
                } else if (col.equals("count")) {
                    Long value = Bytes.toLong(CellUtil.cloneValue(cell));
                    count[cindex++] = value;
                }
            }
        }

        for (int i=0; i<runtime.length; i++){
            Product p = new Product();
            p.setProductName(runtime[i]);
            p.setNums(count[i]);
            productArr.add(p);
        }

        return productArr;
    }

}
