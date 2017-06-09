//package com.sohu.hbasedata.aggregate.mr.HbaseRead;
//
//import com.google.common.collect.Maps;
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.joda.time.DateTime;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Map;
//
///**
// * Created by zengxiaosen on 2017/6/7.
// */
//public class HBaseReadTableReducer1 extends TableReducer<Text, Text, ImmutableBytesWritable> {
//
//    private static final Logger LOG = LoggerFactory.getLogger(HBaseReadTableReducer1.class);
//
//    private static final String SEPA = "\t";
//    private static final String KEY_SEPA = "|";
//    private static final String VALUE_SEPA = "|";
//
//    private static Map<Integer, Long> requestNumResult = Maps.newHashMap();
//    private static Map<Integer, Double> responseTimeResult = Maps.newHashMap();
//    private static Map<Integer, Long> bodySizeResult = Maps.newHashMap();
//    private static Map<Integer, Long> XX2Result = Maps.newHashMap();
//    private static Map<Integer, Long> XX3Result = Maps.newHashMap();
//    private static Map<Integer, Long> XX4Result = Maps.newHashMap();
//    private static Map<Integer, Long> XX5Result = Maps.newHashMap();
//
//
//    protected void reduce(Text rowKey, Iterable<Text> columns, Context context) throws IOException, InterruptedException{
////        try{
////            for (IntWritable values : columns) {
////                System.out.println(values);
////            }
////            Put put = new Put(Bytes.toBytes(rowKey.toString()));
////            put.add(Bytes.toBytes("request"), Bytes.toBytes("col-1"),
////                    Bytes.toBytes(rowKey.toString()));
////            context.write(null, put);
////        } catch (Exception e) {
////            e.printStackTrace();
////        }
//        for (Text value : values) {
//            String[] valueFields = StringUtils.split(value.toString(), VALUE_SEPA);
//
//            long ts = Long.valueOf(valueFields[0]);
//            long requestNum = Long.valueOf(valueFields[1]);
//            double responseTime = Double.valueOf(valueFields[2]);
//            long bodySize = Long.valueOf(valueFields[3]);
//            long XX2_num = Long.valueOf(valueFields[4]);
//            long XX3_num = Long.valueOf(valueFields[5]);
//            long XX4_num = Long.valueOf(valueFields[6]);
//            long XX5_num = Long.valueOf(valueFields[7]);
//
//            DateTime dt = new DateTime(ts);
//
//            // 每5分钟1个点，这里计算是当天的第几个点
//            int pointOfDay = dt.getHourOfDay() * 12 + dt.getMinuteOfHour() / 5 + 1;
//
//            saveToMap(requestNumResult, pointOfDay, requestNum);
//            saveToMap(responseTimeResult, pointOfDay, responseTime);
//            saveToMap(bodySizeResult, pointOfDay, bodySize);
//            saveToMap(XX2Result, pointOfDay, XX2_num);
//            saveToMap(XX3Result, pointOfDay, XX3_num);
//            saveToMap(XX4Result, pointOfDay, XX4_num);
//            saveToMap(XX5Result, pointOfDay, XX5_num);
//        }
//
//    }
//
//}
