package com.sohu.hbasedata.aggregate;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by zengxiaosen on 2017/6/7.
 */
public class HbaseOperation {


    public static void getDataOfTable_Rowkey_Cf(String tableNameStr, ImmutableBytesWritable rowKey, String colFamily, Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context) throws IOException {
        ArrayList<String> list = new ArrayList<String>();
        HTable table = new HTable(context.getConfiguration(), tableNameStr);
        //根据主键查询
        Get get = new Get(rowKey.get());
        Result r = table.get(get);
        KeyValue[] kv = r.raw();
        for(int i=0; i< kv.length; i++){

        }
    }
}
