package com.sohu.hbasedata.aggregate.mr.HbaseFindMaxBandWidth;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;


/**
 * Created by zengxiaosen on 2017/6/7.
 */
public class HBaseFindMaxBandWidthMapper extends TableMapper<Text, Text> {
    private ResultScanner rs=null;
    //private final IntWritable ONE = new IntWritable(1);
    private LinkedHashMap<Integer, ArrayList<Double>> time_cfn = new LinkedHashMap<Integer, ArrayList<Double>>();

    private Text outkey = new Text();
    private Text outValue = new Text();
    @SuppressWarnings("deprecation")
    @Override
    protected void map(ImmutableBytesWritable rowKey, Result columns, Context context) throws IOException, InterruptedException{
        // TODO Auto-generated method stub
        System.out.println("-------------------------------");
        String tableNameStr = context.getConfiguration().get("tableName");


        try {
            String rowkey_md5 = Bytes.toString(rowKey.get());
            HashMap<String, String> customerMap = new HashMap<String, String>();
            KeyValue[] kv = columns.raw();
            time_cfn = new LinkedHashMap<Integer, ArrayList<Double>>(288);

            for(int i=0; i< kv.length; i++){
                String qualifier = "";
                HashMap<?, ?> kvMap = (HashMap<?, ?>) kv[i].toStringMap();

                String mycolumn = new String(kv[i].getFamily());
                String myvalue = new String(kv[i].getValue());
                System.out.println("column " + mycolumn + " data " + myvalue);
                if(mycolumn.equals("2XX")){
                    Integer colid = i % 288;
                    ArrayList<Double> value = time_cfn.get(colid);
                    if(value == null){
                        value = new ArrayList<Double>();
                    }
                    //value.set(0, Integer.valueOf(myvalue.trim()));
                    value.add(Double.valueOf(myvalue.trim()));
                    time_cfn.put(colid, value);
                }else if(mycolumn.equals("3XX")){
                    Integer colid = i % 288;
                    ArrayList<Double> value = time_cfn.get(colid);
                    if(value == null){
                        value = new ArrayList<Double>();
                    }
                    //value.set(1, Integer.valueOf(myvalue.trim()));
                    value.add(Double.valueOf(myvalue.trim()));
                    time_cfn.put(colid, value);
                }else if(mycolumn.equals("4XX")){
                    Integer colid = i % 288;
                    ArrayList<Double> value = time_cfn.get(colid);
                    if(value == null){
                        value = new ArrayList<Double>();
                    }
                    //value.set(2, Integer.valueOf(myvalue.trim()));
                    value.add(Double.valueOf(myvalue.trim()));
                    time_cfn.put(colid, value);
                }else if(mycolumn.equals("5XX")){
                    Integer colid = i % 288;
                    ArrayList<Double> value = time_cfn.get(colid);
                    if(value == null){
                        value = new ArrayList<Double>();
                    }
                    //value.set(3, Integer.valueOf(myvalue.trim()));
                    value.add(Double.valueOf(myvalue.trim()));
                    time_cfn.put(colid, value);
                }
//                else if(mycolumn.equals("bandwidth")){
//                    Integer colid = i % 288;
//                    ArrayList<Double> value = time_cfn.get(colid);
//                    if(value == null){
//                        value = new ArrayList<Double>();
//                    }
//                    value.add(Double.valueOf(myvalue.trim()));
//                    time_cfn.put(colid, value);
//                }
                else if(mycolumn.equals("flow")){
                    Integer colid = i % 288;
                    ArrayList<Double> value = time_cfn.get(colid);
                    if(value == null){
                        value = new ArrayList<Double>();
                    }
                    //value.set(4, Integer.valueOf(myvalue.trim()));
                    value.add(Double.valueOf(myvalue.trim()));
                    time_cfn.put(colid, value);
                }else if(mycolumn.equals("latency")){
                    Integer colid = i % 288;
                    ArrayList<Double> value = time_cfn.get(colid);
                    if(value == null){
                        value = new ArrayList<Double>();
                    }
                    //value.set(5, Integer.valueOf(myvalue.trim()));
                    if(myvalue.trim().equals("0.0")){
                        value.add(Double.valueOf("0"));
                    }else{
                        value.add(Double.valueOf(myvalue.trim()));
                    }
                    time_cfn.put(colid, value);
                }else if(mycolumn.equals("request")){
                    Integer colid = i % 288;
                    ArrayList<Double> value = time_cfn.get(colid);
                    if(value == null){
                        value = new ArrayList<Double>();
                    }
                    //value.set(6, Integer.valueOf(myvalue.trim()));
                    value.add(Double.valueOf(myvalue.trim()));
                    time_cfn.put(colid, value);
                }

            }

            /**
             * HashMap<Integer, ArrayList<Integer>> time_cfn
             *      list
             * key  cf1 cf2 cf3 cf4 cf5 cf6 cf7
             * 1
             * 2
             * ...
             * 288
             */
            System.out.println("time_cfn: ==========================================");
            System.out.println(time_cfn.toString());


            outkey.set(rowkey_md5);
            Gson gson = new Gson();
            String valuejson = gson.toJson(time_cfn);
            outValue.set(valuejson);
            //context.write(text, time_cfn);
            context.write(outkey, outValue);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }


}
