package com.kafka.sparkstreaming.functions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

/**
 * Created by Nagendra on 4/10/16.
 */
public class HbaseSaveFunction implements Function<JavaPairRDD<ImmutableBytesWritable, Put>, Void> {

    final private  Configuration hbaseconf;
    final private Configuration jobConf;
    public HbaseSaveFunction() {

        hbaseconf = HBaseConfiguration.create();
        hbaseconf.set(TableOutputFormat.OUTPUT_TABLE, "meter");
        hbaseconf.set("hbase.zookeeper.quorum", "localhost");
        hbaseconf.set("hbase.zookeeper.property.clientPort","2182");

        jobConf  = new Configuration(hbaseconf);
        jobConf.set("mapreduce.job.output.key.class", "Text");
        jobConf.set("mapreduce.job.output.value.class", "Text");
        jobConf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
    }

    @Override
    public Void call(JavaPairRDD<ImmutableBytesWritable, Put> immutableBytesWritablePutJavaPairRDD) throws Exception {
        immutableBytesWritablePutJavaPairRDD.saveAsNewAPIHadoopDataset(jobConf);
        return null;
    }
}
