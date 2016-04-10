package com.kafka.sparkstreaming.functions;

import com.kafka.sparkstreaming.model.Meter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Random;

/**
 * Created by Nagendra on 4/10/16.
 */
public class HbasePutConvertFunction implements PairFunction<Meter, ImmutableBytesWritable, Put> {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Meter meter) throws Exception {
        Put put = new Put(Bytes.toBytes(meter.getMeterID()));
        put.add(Bytes.toBytes("meter_family"), Bytes.toBytes("MeterID"), Bytes.toBytes(meter.getMeterID()));
        put.add(Bytes.toBytes("meter_family"), Bytes.toBytes("MeterStatus"), Bytes.toBytes(meter.getMeterStatus()));
        put.add(Bytes.toBytes("meter_family"), Bytes.toBytes("MeterTime"), Bytes.toBytes(meter.getMeterTime()));
        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }
}
