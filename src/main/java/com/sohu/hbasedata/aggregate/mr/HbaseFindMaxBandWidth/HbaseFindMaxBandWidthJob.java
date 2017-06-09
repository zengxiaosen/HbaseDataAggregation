package com.sohu.hbasedata.aggregate.mr.HbaseFindMaxBandWidth;

/**
 * Created by zengxiaosen on 2017/6/7.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//读hbase数据
public class HbaseFindMaxBandWidthJob {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseFindMaxBandWidthJob.class);
    public static void main(String[] args) throws Exception{
        LOG.debug("start");

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));
        //conf.addResource(new Path("/user/xiaoliu/hbase-site.xml"));

        LOG.debug("got conf");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("Usage: HbaseFindMaxBandWidthJob <table> <table>");
            System.out.println(2);
        }
        //cdn:zengxiaosen
        String tableNameInput = otherArgs[0];
        String tableNameOutput = otherArgs[1];
        conf.set("tableName", tableNameInput);
        conf.set("tableOutputName", tableNameOutput);
        Job job = new Job(conf, "HbaseFindMaxBandWidthJob");
        job.setJarByClass(HbaseFindMaxBandWidthJob.class);
        LOG.debug("init credential");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        //设置读取hbase的相关操作
        //输入表
        TableMapReduceUtil.initTableMapperJob(tableNameInput,
                scan,// Scan instance to control CF and attribute selection
                HBaseFindMaxBandWidthMapper.class,// mapper class
                Text.class,// mapper output key
                Text.class,// mapper output value
                job);

        job.setOutputFormatClass(NullOutputFormat.class); // because we aren't


        job.setReducerClass(HBaseFindMaxBandWidthReducer.class);


        //输出表
        TableMapReduceUtil.initTableReducerJob(tableNameOutput,
                HBaseFindMaxBandWidthReducer.class,
                job);
        job.waitForCompletion(true);

    }
}
