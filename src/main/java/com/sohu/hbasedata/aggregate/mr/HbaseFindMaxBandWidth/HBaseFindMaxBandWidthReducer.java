package com.sohu.hbasedata.aggregate.mr.HbaseFindMaxBandWidth;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by zengxiaosen on 2017/6/7.
 */
public class HBaseFindMaxBandWidthReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    private LinkedHashMap<Integer, ArrayList<Double>> object = new LinkedHashMap<Integer, ArrayList<Double>>();
    private LinkedHashMap<Integer, ArrayList<Double>> collectionResult = new LinkedHashMap<Integer, ArrayList<Double>>();
    protected void reduce(Text rowKey, Iterable<Text> columns, Context context) throws IOException, InterruptedException{

        try{

            if(columns.iterator().hasNext()){
                //object = columns.iterator().next();
                Gson gson = new Gson();
                Text value = columns.iterator().next();
                object = gson.fromJson(value.toString(), LinkedHashMap.class);

            }
            Integer count = 0;
            Integer count1 = 0;
            Double cf1 = 0.0;
            Double cf2 = 0.0;
            Double cf3 = 0.0;
            Double cf4 = 0.0;

            Double cf5 = 0.0;
            Double cf6 = 0.0;
            Double cf7 = 0.0;

            for(Map.Entry<Integer, ArrayList<Double>> entry: object.entrySet()){
                //Integer key = entry.getKey();
                ArrayList<Double> value = entry.getValue();
                if(count % 4 == 0){
                    count = 0;
                    cf1 = 0.0;
                    cf2 = 0.0;
                    cf3 = 0.0;
                    cf4 = 0.0;
                    cf5 = 0.0;
                    cf6 = 0.0;
                    cf7 = 0.0;
                    count1 ++;

                }

                cf1 += value.get(0);
                cf2 += value.get(1);
                cf3 += value.get(2);
                cf4 += value.get(3);
                cf5 += value.get(4);
                cf6 += value.get(5);
                cf7 += value.get(6);


                if(count % 4 == 3){
                    //加够了
                    ArrayList<Double> temp = new ArrayList<Double>();
//                    temp.set(0, cf1);
//                    temp.set(1, cf2);
//                    temp.set(2, cf3);
//                    temp.set(3, cf4);
//                    temp.set(4, cf5);
//                    temp.set(5, cf6);
//                    temp.set(6, cf7);
                    temp.add(cf1);
                    temp.add(cf2);
                    temp.add(cf3);
                    temp.add(cf4);
                    temp.add(cf5);
                    temp.add(cf6);
                    temp.add(cf7);
                    collectionResult.put(count1, temp);
                }

                count ++;
            }
            //72
            System.out.println("count:  " + count);

            /**
             * HashMap<Integer, ArrayList<Integer>> collectionResult
             *      list
             * key  cf1 cf2 cf3 cf4 cf5 cf6 cf7
             * 1
             * 2
             * ...
             * 72
             */


        } catch (Exception e) {
            e.printStackTrace();
        }

        String tableOutputName = context.getConfiguration().get("tableOutputName");
        HTable table = new HTable(context.getConfiguration(), tableOutputName);

        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        if(!result.isEmpty()){
            //waiting to finish
        }
        System.out.println("rowKey" + rowKey.toString());
        Put put = new Put(Bytes.toBytes(rowKey.toString()));
        Integer count2 = 0;
        for(Map.Entry<Integer, ArrayList<Double>> entry: collectionResult.entrySet()){
            Integer key = entry.getKey();
            ArrayList<Double> value = entry.getValue();
            //saveToPut(put, key, value);
            if(value.get(0)!=null){
                put.add(Bytes.toBytes("2XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(0).toString()));
            }else{
                put.add(Bytes.toBytes("2XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
            }

            if(value.get(1)!=null){
                put.add(Bytes.toBytes("3XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(1).toString()));
            }else{
                put.add(Bytes.toBytes("3XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
            }

            if(value.get(2)!=null){
                put.add(Bytes.toBytes("4XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(2).toString()));
            }else{
                put.add(Bytes.toBytes("4XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
            }

            if(value.get(3)!=null){
                put.add(Bytes.toBytes("5XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(3).toString()));
            }else{
                put.add(Bytes.toBytes("5XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
            }

            //bandwidth
//        if(value.get(4)!=null){
//            put.add(Bytes.toBytes("bandwidth"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(4).toString()));
//        }else{
//            put.add(Bytes.toBytes("bandwidth"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
//        }

            if(value.get(4)!=null){
                put.add(Bytes.toBytes("flow"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(4).toString()));
            }else{
                put.add(Bytes.toBytes("flow"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
            }

            if(value.get(5)!=null){
                put.add(Bytes.toBytes("latency"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(5).toString()));
            }else{
                put.add(Bytes.toBytes("latency"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
            }

            if(value.get(6)!=null){
                put.add(Bytes.toBytes("request"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(6).toString()));
            }else{
                put.add(Bytes.toBytes("request"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
            }
            //===================================================================================================
//            System.out.println("key: " + key);
//            System.out.println("value: " + value.toString());
//            System.out.println("count2" + count2);

            count2 ++;
        }

        context.write(null, put);


    }

    private void saveToPut(Put put, Integer key, ArrayList<Double> value) {
        if(value.get(0)!=null){
            put.add(Bytes.toBytes("2XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(0).toString()));
        }else{
            put.add(Bytes.toBytes("2XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
        }

        if(value.get(1)!=null){
            put.add(Bytes.toBytes("3XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(1).toString()));
        }else{
            put.add(Bytes.toBytes("3XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
        }

        if(value.get(2)!=null){
            put.add(Bytes.toBytes("4XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(2).toString()));
        }else{
            put.add(Bytes.toBytes("4XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
        }

        if(value.get(3)!=null){
            put.add(Bytes.toBytes("5XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(3).toString()));
        }else{
            put.add(Bytes.toBytes("5XX"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
        }

        //bandwidth
//        if(value.get(4)!=null){
//            put.add(Bytes.toBytes("bandwidth"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(4).toString()));
//        }else{
//            put.add(Bytes.toBytes("bandwidth"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
//        }

        if(value.get(4)!=null){
            put.add(Bytes.toBytes("flow"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(4).toString()));
        }else{
            put.add(Bytes.toBytes("flow"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
        }

        if(value.get(5)!=null){
            put.add(Bytes.toBytes("latency"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(5).toString()));
        }else{
            put.add(Bytes.toBytes("latency"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
        }

        if(value.get(6)!=null){
            put.add(Bytes.toBytes("request"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes(value.get(6).toString()));
        }else{
            put.add(Bytes.toBytes("request"), Bytes.toBytes(String.valueOf(key)), Bytes.toBytes("0"));
        }


    }

}
