package com.diguage.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author diguage
 * @date 2017-11-27
 */
public class HelloWorld {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("HelloWorld");
        JavaSparkContext context = new JavaSparkContext(conf);

        Broadcast<int[]> broadcast = context.broadcast(new int[]{1, 2, 3});

        LongAccumulator accumulator = context.sc().longAccumulator();
        accumulator.add(123L);


        JavaRDD<String> lines = context.textFile("/Users/diguage/init.sh");
        JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.split(" ")).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        JavaPairRDD<String, Integer> sortCount = counts.sortByKey();
        List<Tuple2<String, Integer>> collect = sortCount.collect();
        for (Tuple2<String, Integer> tuple2 : collect) {
            System.out.println(tuple2._1() + "\t\t" + tuple2._2());
        }

    }
}
